const _ = require('lodash');
const stream = require('stream');
// const mssql = require('mssql');
const tedious = require('tedious');
const driverBase = require('../frontend/driver');
const MsSqlAnalyser = require('./MsSqlAnalyser');
const createBulkInsertStream = require('./createBulkInsertStream');

function extractColumns(columns) {
  const res = columns.map((col) => {
    const resCol = {
      columnName: col.colName,
      dataType: col.type.name.toLowerCase(),
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

/** @type {import('dbgate-types').EngineDriver} */
const driver = {
  ...driverBase,
  analyserClass: MsSqlAnalyser,
  async connect({ server, port, user, password, database }) {
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
      connection.on('connect', function (err) {
        if (err) {
          reject(err);
        }
        resolve(connection);
      });
      connection.connect();
    });
  },
  // @ts-ignore
  async query(pool, sql) {
    if (sql == null) {
      return {
        rows: [],
        columns: [],
      };
    }
    return new Promise((resolve, reject) => {
      const result = {
        rows: [],
        columns: [],
      };
      const request = new tedious.Request(sql, (err, rowCount) => {
        if (err) reject(err);
        else resolve(result);
      });
      request.on('columnMetadata', function (columns) {
        result.columns = extractColumns(columns);
      });
      request.on('row', function (columns) {
        result.rows.push(
          _.zipObject(
            result.columns.map((x) => x.columnName),
            columns.map((x) => x.value)
          )
        );
      });
      pool.execSql(request);
    });
  },
  async stream(pool, sql, options) {
    let currentColumns = [];

    const handleInfo = (info) => {
      const { message, lineNumber, procName } = info;
      options.info({
        message,
        line: lineNumber,
        procedure: procName,
        time: new Date(),
        severity: 'info',
      });
    };
    const handleError = (error) => {
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
    });
    request.on('columnMetadata', function (columns) {
      currentColumns = extractColumns(columns);
      options.recordset(currentColumns);
    });
    request.on('row', function (columns) {
      const row = _.zipObject(
        currentColumns.map((x) => x.columnName),
        columns.map((x) => x.value)
      );
      options.row(row);
    });
    pool.execSql(request);

  },
  async readQuery(pool, sql, structure) {
    const request = await pool.request();

    const pass = new stream.PassThrough({
      objectMode: true,
      highWaterMark: 100,
    });

    request.stream = true;
    request.on('recordset', (driverColumns) => {
      const [columns, mapper] = extractColumns(driverColumns);
      pass.write(structure || { columns });
    });
    request.on('row', (row) => pass.write(row));
    request.on('error', (err) => {
      console.error(err);
      pass.end();
    });
    request.on('done', () => pass.end());

    request.query(sql);

    return pass;
  },
  async writeTable(pool, name, options) {
    return createBulkInsertStream(this, mssql, stream, pool, name, options);
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

module.exports = driver;
