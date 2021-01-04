const { SqlDumper } = require('dbgate-tools');

class MsSqlDumper extends SqlDumper {
  autoIncrement() {
    this.put(' ^identity');
  }

  putStringValue(value) {
    if (/[^\u0000-\u00ff]/.test(value)) {
      this.putRaw('N');
    }
    super.putStringValue(value);
  }

  allowIdentityInsert(table, allow) {
    this.putCmd('^set ^identity_insert %f %k;&n', table, allow ? 'on' : 'off');
  }

  /** @param type {import('dbgate-types').TransformType} */
  transform(type, dumpExpr) {
    switch (type) {
      case 'GROUP:YEAR':
      case 'YEAR':
        this.put('^datepart(^year, %c)', dumpExpr);
        break;
      case 'MONTH':
        this.put('^datepart(^month, %c)', dumpExpr);
        break;
      case 'DAY':
        this.put('^datepart(^day, %c)', dumpExpr);
        break;
      case 'GROUP:MONTH':
        this.put(
          "^convert(^varchar(100), ^datepart(^year, %c)) + '-' + right('0' + ^convert(^varchar(100), ^datepart(^month, %c)), 2)",
          dumpExpr,
          dumpExpr
        );
        break;
      case 'GROUP:DAY':
        this.put(
          "^^convert(^varchar(100), ^datepart(^year, %c)) + '-' + ^right('0' + ^convert(^varchar(100), ^datepart(^month, %c)), 2)+'-' + ^right('0' + ^convert(^varchar(100), ^datepart(^day, %c)), 2)",
          dumpExpr,
          dumpExpr,
          dumpExpr
        );
        break;
      default:
        dumpExpr();
        break;
    }
  }
}

module.exports = MsSqlDumper;
