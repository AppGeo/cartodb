'use strict';

var Interface = require('../interface');
var inherits = require('inherits');
var debug = require('debug')('cartodb:schemaBuilder');
// Constructor for the builder instance, typically called from
// `knex.builder`, accepting the current `knex` instance,
// and pulling out the `client` and `grammar` from the current
// knex instance.
function SchemaBuilder(client) {
  this.client = client;
  this._sequence = [];
  this._debug = client.config && client.config.debug;
}
inherits(SchemaBuilder, Interface);
var methods = ['createTable', 'createTableIfNotExists', 'createSchema', 'createSchemaIfNotExists', 'dropSchema', 'dropSchemaIfExists', 'createExtension', 'createExtensionIfNotExists', 'dropExtension', 'dropExtensionIfExists', 'table', 'alterTable', 'hasTable', 'hasColumn', 'dropTable', 'renameTable', 'dropTableIfExists', 'raw'];
// Each of the schema builder methods just add to the
// "_sequence" array for consistency.
methods.forEach(function (method) {
  SchemaBuilder.prototype[method] = function () {
    if (method === 'table') {
      method = 'alterTable';
    }
    var args = new Array(arguments.length);
    var i = -1;
    while (++i < arguments.length) {
      args[i] = arguments[i];
    }
    this._sequence.push({
      method: method,
      args: args
    });
    return this;
  };
});


SchemaBuilder.prototype.toString = function () {
  return this.toQuery();
};

SchemaBuilder.prototype.toSQL = function () {
  var compiler = this.client.schemaCompiler(this);
  var out = compiler.toSQL();
  this._tableName = compiler._tableName;
  return out;
};

module.exports = SchemaBuilder;
