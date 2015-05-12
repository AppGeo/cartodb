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
  return data.map(function (statement) {
    return SqlString.format(statement.sql, statement.bindings, tz, statement.method);
  }, this).join(';\n');
};

Interface.prototype.exec = function (cb) {
  return this.client.exec(this.toQuery(), cb);
};
Interface.prototype.then = function (resolve, reject) {
  var self = this;
  return new Promise(function (resolve, reject) {
    self.client.exec(self.toQuery(), function (err, resp) {
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
