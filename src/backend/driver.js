const _ = require('lodash');
const stream = require('stream');
// const mssql = require('mssql');
const tedious = require('tedious');
const driverBase = require('../frontend/driver');
const MsSqlAnalyser = require('./MsSqlAnalyser');
const createBulkInsertStream = require('./createBulkInsertStream');
const AsyncLock = require('async-lock');
const nativeDriver = require('./nativeDriver');
const lock = new AsyncLock();
const { tediousConnect, tediousQueryCore, tediousReadQuery } = require('./tediousDriver');
const { nativeConnect, nativeQueryCore, nativeReadQuery } = nativeDriver;
let msnodesqlv8;

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
    if (pool._connectionType == 'msnodesqlv8') {
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
      currentColumns = extractTediousColumns(columns);
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
    if (pool._connectionType == 'msnodesqlv8') {
      return nativeReadQuery(pool, sql, structure);
    } else {
      return tediousReadQuery(pool, sql, structure);
    }
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
    msnodesqlv8 = dbgateEnv.nativeModules.msnodesqlv8();
  }
  nativeDriver.initialize(dbgateEnv);
};

module.exports = driver;
