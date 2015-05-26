'use strict';

var helpers = require('./helpers');
var assign = require('../assign');
var debug = require('debug')('cartodb:schemaCompiler');

// The "SchemaCompiler" takes all of the query statements which have been
// gathered in the "SchemaBuilder" and turns them into an array of
// properly formatted / bound query strings.
function SchemaCompiler(client, builder) {
  this.builder = builder;
  this.client = client;
  this.formatter = client.formatter();
  this.sequence = [];
}

assign(SchemaCompiler.prototype, {

  pushQuery: helpers.pushQuery,

  pushAdditional: helpers.pushAdditional,

  createTable: buildTable('create'),

  createTableIfNotExists: buildTable('createIfNot'),

  alterTable: buildTable('alter'),

  dropTable: function dropTable(tableName) {
    this.pushQuery('drop table ' + this.formatter.wrap(tableName));
  },

  dropTableIfExists: function dropTableIfExists(tableName) {
    this.pushQuery('drop table if exists ' + this.formatter.wrap(tableName));
  },

  raw: function raw(sql, bindings) {
    this.sequence.push(this.client.raw(sql, bindings).toSQL());
  },

  toSQL: function toSQL() {
    var sequence = this.builder._sequence;
    for (var i = 0, l = sequence.length; i < l; i++) {
      var query = sequence[i];
      this[query.method].apply(this, query.args);
    }
    debug(this.sequence);
    return this.sequence;
  }

});

function buildTable(type) {
  return function (tableName, fn) {
    if (type === 'create') {
      this._tableName = tableName;
    }
    var sql = this.client.tableBuilder(type, tableName, fn).toSQL();
    for (var i = 0, l = sql.length; i < l; i++) {
      this.sequence.push(sql[i]);
    }
  };
}


// Check whether the current table
SchemaCompiler.prototype.hasTable = function (tableName) {
  this.pushQuery({
    sql: 'select * from information_schema.tables where table_name = ?',
    bindings: [tableName],
    output: function output(resp) {
      return resp.rows.length > 0;
    }
  });
};

// Compile the query to determine if a column exists in a table.
SchemaCompiler.prototype.hasColumn = function (tableName, columnName) {
  this.pushQuery({
    sql: 'select * from information_schema.columns where table_name = ? and column_name = ?',
    bindings: [tableName, columnName],
    output: function output(resp) {
      return resp.rows.length > 0;
    }
  });
};

// Compile a rename table command.
SchemaCompiler.prototype.renameTable = function (from, to) {
  this.pushQuery('alter table ' + this.formatter.wrap(from) + ' rename to ' + this.formatter.wrap(to));
};

SchemaCompiler.prototype.createSchema = function (schemaName) {
  this.pushQuery('create schema ' + this.formatter.wrap(schemaName));
};

SchemaCompiler.prototype.createSchemaIfNotExists = function (schemaName) {
  this.pushQuery('create schema if not exists ' + this.formatter.wrap(schemaName));
};

SchemaCompiler.prototype.dropSchema = function (schemaName) {
  this.pushQuery('drop schema ' + this.formatter.wrap(schemaName));
};

SchemaCompiler.prototype.dropSchemaIfExists = function (schemaName) {
  this.pushQuery('drop schema if exists ' + this.formatter.wrap(schemaName));
};

SchemaCompiler.prototype.dropExtension = function (extensionName) {
  this.pushQuery('drop extension ' + this.formatter.wrap(extensionName));
};

SchemaCompiler.prototype.dropExtensionIfExists = function (extensionName) {
  this.pushQuery('drop extension if exists ' + this.formatter.wrap(extensionName));
};

SchemaCompiler.prototype.createExtension = function (extensionName) {
  this.pushQuery('create extension ' + this.formatter.wrap(extensionName));
};

SchemaCompiler.prototype.createExtensionIfNotExists = function (extensionName) {
  this.pushQuery('create extension if not exists ' + this.formatter.wrap(extensionName));
};

module.exports = SchemaCompiler;
