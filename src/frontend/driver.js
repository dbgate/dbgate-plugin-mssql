const { driverBase } = require('dbgate-tools');
const MsSqlDumper = require('./MsSqlDumper');

/** @type {import('dbgate-types').SqlDialect} */
const dialect = {
  limitSelect: true,
  rangeSelect: true,
  offsetFetchRangeSyntax: true,
  stringEscapeChar: "'",
  fallbackDataType: 'nvarchar(max)',
  quoteIdentifier(s) {
    return `[${s}]`;
  },
};

/** @type {import('dbgate-types').EngineDriver} */
const driver = {
  ...driverBase,
  dumperClass: MsSqlDumper,
  dialect,
  engine: 'mssql@dbgate-plugin-mssql',
  title: 'Microsoft SQL Server',
  defaultPort: 1433,
};

module.exports = driver;
