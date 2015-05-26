
// Column Compiler
// Used for designating column definitions
// during the table "create" / "alter" statements.
// -------
'use strict';

var Raw = require('../raw');
var helpers = require('./helpers');

function ColumnCompiler(client, tableCompiler, columnBuilder) {
  this.client = client;
  this.tableCompiler = tableCompiler;
  this.columnBuilder = columnBuilder;
  this.args = columnBuilder._args;
  this.type = columnBuilder._type.toLowerCase();
  var grouped = this.grouped = {};
  columnBuilder._statements.forEach(function (item) {
    if (!item.grouping) {
      return;
    }
    var val = item.grouping;
    if (!(val in grouped)) {
      grouped[val] = [];
    }
    grouped[val].push(item);
  });
  this.modified = columnBuilder._modifiers;
  this.isIncrements = this.type.indexOf('increments') !== -1;
  this.formatter = client.formatter();
  this.sequence = [];
  this.modifiers = ['nullable', 'defaultTo', 'comment'];
}

ColumnCompiler.prototype.pushQuery = helpers.pushQuery;

ColumnCompiler.prototype.pushAdditional = helpers.pushAdditional;

// To convert to sql, we first go through and build the
// column as it would be in the insert statement
ColumnCompiler.prototype.toSQL = function () {
  this.pushQuery(this.compileColumn());
  if (this.sequence.additional) {
    this.sequence = this.sequence.concat(this.sequence.additional);
  }
  return this.sequence;
};

// Compiles a column.
ColumnCompiler.prototype.compileColumn = function () {
  return this.formatter.wrap(this.getColumnName()) + ' ' + this.getColumnType() + this.getModifiers();
};

// Assumes the autoincrementing key is named `id` if not otherwise specified.
ColumnCompiler.prototype.getColumnName = function () {
  var value = this.args[0];
  if (value) {
    return value;
  }
  if (this.isIncrements) {
    return 'id';
  } else {
    throw new Error('You did not specify a column name for the ' + this.type + 'column.');
  }
};

ColumnCompiler.prototype.getColumnType = function () {
  var type = this[this.type];
  return typeof type === 'function' ? type.apply(this, this.args.slice(1)) : type;
};

ColumnCompiler.prototype.getModifiers = function () {
  var modifiers = [];
  if (this.type.indexOf('increments') === -1) {
    for (var i = 0, l = this.modifiers.length; i < l; i++) {
      var modifier = this.modifiers[i];
      if (this.modified[modifier]) {
        var val = this[modifier].apply(this, this.modified[modifier]);
        if (val) {
          modifiers.push(val);
        }
      }
    }
  }
  return modifiers.length > 0 ? ' ' + modifiers.join(' ') : '';
};

// Types
// ------

ColumnCompiler.prototype.increments = 'serial primary key';
ColumnCompiler.prototype.bigincrements = 'bigserial primary key';
ColumnCompiler.prototype.integer = 'integer';
ColumnCompiler.prototype.smallint = 'smallint';
ColumnCompiler.prototype.tinyint = 'smallint';
ColumnCompiler.prototype.mediumint = 'integer';
ColumnCompiler.prototype.biginteger = 'bigint';
ColumnCompiler.prototype.varchar = function (length) {
  return 'varchar(' + this._num(length, 255) + ')';
};
ColumnCompiler.prototype.text = 'text';
ColumnCompiler.prototype.floating = 'real';
ColumnCompiler.prototype.double = 'double precision';
ColumnCompiler.prototype.decimal = function (precision, scale) {
  return 'decimal(' + this._num(precision, 8) + ', ' + this._num(scale, 2) + ')';
};
ColumnCompiler.prototype.binary = 'bytea';
ColumnCompiler.prototype.bool = 'boolean';
ColumnCompiler.prototype.date = 'date';
ColumnCompiler.prototype.datetime = function datetime(without) {
  return without ? 'timestamp' : 'timestamptz';
};
ColumnCompiler.prototype.time = 'time';
ColumnCompiler.prototype.timestamp = function timestamp(without) {
  return without ? 'timestamp' : 'timestamptz';
};
ColumnCompiler.prototype.enu = function enu(allowed) {
  return 'text check (' + this.formatter.wrap(this.args[0]) + ' in (\'' + allowed.join('\', \'') + '\'))';
};

ColumnCompiler.prototype.bit = function bit(column) {
  return column.length !== false ? 'bit(' + column.length + ')' : 'bit';
};
ColumnCompiler.prototype.json = function json(jsonb) {
  return jsonb ? 'jsonb' : 'json';
};

ColumnCompiler.prototype.uuid = 'uuid';
ColumnCompiler.prototype.comment = function comment(_comment) {
  this.pushAdditional(function () {
    this.pushQuery('comment on column ' + this.tableCompiler.tableName() + '.' + this.formatter.wrap(this.args[0]) + ' is ' + (_comment ? '\'' + _comment + '\'' : 'NULL'));
  }, _comment);
};
ColumnCompiler.prototype.specifictype = function (type) {
  return type;
};

// Modifiers
// -------

ColumnCompiler.prototype.nullable = function (nullable) {
  return nullable === false ? 'not null' : 'null';
};
ColumnCompiler.prototype.notNullable = function () {
  return this.nullable(false);
};
ColumnCompiler.prototype.defaultTo = function (value) {
  if (value === void 0) {
    return '';
  } else if (value === null) {
    value = 'null';
  } else if (value instanceof Raw) {
    value = value.toQuery();
  } else if (this.type === 'bool') {
    if (value === 'false') {
      value = 0;
    }
    value = '\'' + (value ? 1 : 0) + '\'';
  } else if (this.type === 'json' && typeof value === 'object') {
    return JSON.stringify(value);
  } else {
    value = '\'' + value + '\'';
  }
  return 'default ' + value;
};
ColumnCompiler.prototype._num = function (val, fallback) {
  if (val === undefined || val === null) {
    return fallback;
  }
  var number = parseInt(val, 10);
  return isNaN(number) ? fallback : number;
};

module.exports = ColumnCompiler;
