const _ = require('lodash');
const stream = require('stream');
// const mssql = require('mssql');
const tedious = require('tedious');
const driverBase = require('../frontend/driver');
const MsSqlAnalyser = require('./MsSqlAnalyser');
const createBulkInsertStream = require('./createBulkInsertStream');
const AsyncLock = require('async-lock');
const lock = new AsyncLock();
let msnodesqlv8;

function extractColumns(columns, addDriverNativeColumn = false) {
  const res = columns.map(col => {
    const resCol = {
      columnName: col.colName,
      dataType: col.type.name.toLowerCase(),
      driverNativeColumn: addDriverNativeColumn ? col : undefined,

      notNull: !(col.flags & 0x01),
      autoIncrement: !!(col.flags & 0x10),
    };
    if (col.dataLength) resCol.dataType += `(${col.dataLength})`;
    return resCol;
  });

  const usedNames = new Set();
  for (let i = 0; i < res.length; i++) {
    if (usedNames.has(res[i].columnName)) {
      let suffix = 2;
      while (res.has(`${res[i].columnName}${suffix}`)) suffix++;
      res[i].columnName = `${res[i].columnName}${suffix}`;
    }
    usedNames.add(res[i].columnName);
  }

  return res;
}

async function tediousConnect({ server, port, user, password, database }) {
  return new Promise((resolve, reject) => {
    const connection = new tedious.Connection({
      server,

      authentication: {
        type: 'default',
        options: {
          userName: user,
          password: password,
        },
      },

      options: {
        encrypt: false,
        enableArithAbort: true,
        validateBulkLoadParameters: false,
        requestTimeout: 1000 * 3600,
        database,
        port,
      },
    });
    connection.on('connect', function(err) {
      if (err) {
        reject(err);
      }
      resolve(connection);
    });
    connection.connect();
  });
}

async function nativeConnect({ server, port, user, password, database, authType }) {
  let connectionString = `server=${server}`;
  if (port && !server.includes('\\')) connectionString += `,${port}`;
  connectionString += ';Driver={SQL Server Native Client 11.0}';
  if (authType == 'sspi') connectionString += ';Trusted_Connection=Yes';
  else connectionString += `;UID=${user};PWD=${password}`;
  if (database) connectionString += `;Database=${database}`;
  return new Promise((resolve, reject) => {
    msnodesqlv8.open(connectionString, (err, conn) => {
      if (err) reject(err);
      resolve(conn);
    });
  });
}

async function tediousQueryCore(pool, sql, options) {
  if (sql == null) {
    return Promise.resolve({
      rows: [],
      columns: [],
    });
  }
  const { addDriverNativeColumn } = options || {};
  return new Promise((resolve, reject) => {
    const result = {
      rows: [],
      columns: [],
    };
    const request = new tedious.Request(sql, (err, rowCount) => {
      if (err) reject(err);
      else resolve(result);
    });
    request.on('columnMetadata', function(columns) {
      result.columns = extractColumns(columns, addDriverNativeColumn);
    });
    request.on('row', function(columns) {
      result.rows.push(
        _.zipObject(
          result.columns.map(x => x.columnName),
          columns.map(x => x.value)
        )
      );
    });
    pool.execSql(request);
  });
}

async function nativeQueryCore(pool, sql, options) {
  if (sql == null) {
    return Promise.resolve({
      rows: [],
      columns: [],
    });
  }
  return new Promise((resolve, reject) => {
    pool.query(sql, (err, rows) => {
      if (err) reject(err);
      resolve({
        rows,
      });
    });
  });
}

/** @type {import('dbgate-types').EngineDriver} */
const driver = {
  ...driverBase,
  analyserClass: MsSqlAnalyser,
  async connect(conn) {
    const { authType } = conn;
    if (msnodesqlv8 && (authType == 'sspi' || authType == 'sql')) {
      return nativeConnect(conn);
    }

    return tediousConnect(conn);
  },
  async queryCore(pool, sql, options) {
    if (pool.constructor.name == 'ConnectionWrapper') {
      return nativeQueryCore(pool, sql, options);
    } else {
      return tediousQueryCore(pool, sql, options);
    }
  },
  async query(pool, sql, options) {
    return lock.acquire('connection', async () => {
      return this.queryCore(pool, sql, options);
    });
  },
  async stream(pool, sql, options) {
    let currentColumns = [];

    const handleInfo = info => {
      const { message, lineNumber, procName } = info;
      options.info({
        message,
        line: lineNumber,
        procedure: procName,
        time: new Date(),
        severity: 'info',
      });
    };
    const handleError = error => {
      const { message, lineNumber, procName } = error;
      options.info({
        message,
        line: lineNumber,
        procedure: procName,
        time: new Date(),
        severity: 'error',
      });
    };

    pool.on('infoMessage', handleInfo);
    pool.on('errorMessage', handleError);
    const request = new tedious.Request(sql, (err, rowCount) => {
      // if (err) reject(err);
      // else resolve(result);
      options.done();
      pool.off('infoMessage', handleInfo);
      pool.off('errorMessage', handleError);

      options.info({
        message: `${rowCount} rows affected`,
        time: new Date(),
        severity: 'info',
      });
    });
    request.on('columnMetadata', function(columns) {
      currentColumns = extractColumns(columns);
      options.recordset(currentColumns);
    });
    request.on('row', function(columns) {
      const row = _.zipObject(
        currentColumns.map(x => x.columnName),
        columns.map(x => x.value)
      );
      options.row(row);
    });
    pool.execSqlBatch(request);
  },
  async readQuery(pool, sql, structure) {
    const pass = new stream.PassThrough({
      objectMode: true,
      highWaterMark: 100,
    });
    let currentColumns = [];

    const request = new tedious.Request(sql, (err, rowCount) => {
      if (err) console.error(err);
      pass.end();
    });
    request.on('columnMetadata', function(columns) {
      currentColumns = extractColumns(columns);
      pass.write(structure || { columns: currentColumns });
    });
    request.on('row', function(columns) {
      const row = _.zipObject(
        currentColumns.map(x => x.columnName),
        columns.map(x => x.value)
      );
      pass.write(row);
    });
    pool.execSql(request);

    return pass;
  },
  async writeTable(pool, name, options) {
    return createBulkInsertStream(this, stream, pool, name, options);
  },
  async getVersion(pool) {
    const { version } = (await this.query(pool, 'SELECT @@VERSION AS version')).rows[0];
    return { version };
  },
  async listDatabases(pool) {
    const { rows } = await this.query(pool, 'SELECT name FROM sys.databases order by name');
    return rows;
  },
};

driver.initialize = dbgateEnv => {
  if (dbgateEnv.nativeModules) {
    msnodesqlv8 = dbgateEnv.nativeModules.msnodesqlv8;
  }
};

module.exports = driver;
