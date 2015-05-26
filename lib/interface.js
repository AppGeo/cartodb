'use strict';
var SqlString = require('./string');
var Promise = require('bluebird');
var debug = require('debug')('cartodb:interface');
module.exports = Interface;
function Interface () {}

Interface.prototype.toQuery = function (tz) {
  var data = this.toSQL(this._method);
  if (!Array.isArray(data)) {
    data = [data];
  }
  debug(data);
  return 'BEGIN;\n' + data.map(function (statement) {
    return SqlString.format(statement.sql, statement.bindings, tz, statement.method);
  }, this).join(';\n') + ';\nCOMMIT;';
};

Interface.prototype.exec = function (cb) {
  var query = this.toQuery();
  var callback = cb;
  var self = this;
  if (this._tableName) {
    callback = function (err, resp) {
      if (err) {
        return cb(err);
      }
      return self.client.exec('select cdb_cartodbfytable(\'' + self._tableName + '\');', function (err) {
        if (err) {
          return cb(err);
        }
        cb(null, resp);
      });
    };
  }
  return this.client.exec(query, callback);
};
Interface.prototype.then = function (resolve, reject) {
  var self = this;
  return new Promise(function (resolve, reject) {
    self.exec(function (err, resp) {
      if (err) {
        return reject(err);
      } else {
        resolve(resp);
      }
    });
  }).then(resolve, reject);
};
Interface.prototype.catch = function (reject) {
  return this.then(null, reject);
};
Interface.prototype.stream = function () {
  return this.exec();
};
