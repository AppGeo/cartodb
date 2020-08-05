// from knex
// Copyright (c) 2013-2014 Tim Griesser
// Raw
// -------
'use strict';

var assign = require('./assign');
var Interface = require('./interface');
var inherits = require('inherits');

inherits(Raw, Interface);

function Raw(client) {
  this.client = client;

  this.sql = '';
  this.bindings = [];
  this._cached = undefined;
  this._noTransaction = false;
  // Todo: Deprecate
  this._wrappedBefore = undefined;
  this._wrappedAfter = undefined;
  this._debug = client && client.options && client.options.debug;
}

assign(Raw.prototype, {

  set: function set(sql, bindings) {
    this._cached = undefined;
    this.sql = sql;
    this.bindings = bindings;
    return this;
  },
  batch: function () {
    this.client.batch();
    return this;
  },
  onSuccess: function (query) {
    this.client.onSuccess(query);
    return this;
  },
  onError: function (query) {
    this.client.onError(query);
    return this;
  },
  // Wraps the current sql with `before` and `after`.
  wrap: function wrap(before, after) {
    this._cached = undefined;
    this._wrappedBefore = before;
    this._wrappedAfter = after;
    return this;
  },

  // Calls `toString` on the Knex object.
  toString: function toString() {
    return this.toQuery();
  },
  noTransaction: function () {
    this._noTransaction = true;
    return this;
  },
  // Returns the raw sql for the query.
  toSQL: function toSQL() {
    if (this._cached) {
      return this._cached;
    }
    if (Array.isArray(this.bindings)) {
      this._cached = replaceRawArrBindings(this);
    } else if (this.bindings && typeof this.bindings === 'object') {
      this._cached = replaceKeyBindings(this);
    } else {
      this._cached = {
        method: 'raw',
        sql: this.sql,
        bindings: this.bindings
      };
    }
    if (this._wrappedBefore) {
      this._cached.sql = this._wrappedBefore + this._cached.sql;
    }
    if (this._wrappedAfter) {
      this._cached.sql = this._cached.sql + this._wrappedAfter;
    }
    this._cached.options = assign({}, this._options);
    return this._cached;
  }

});

function replaceRawArrBindings(raw) {
  var expectedBindings = raw.bindings.length;
  var values = raw.bindings;
  var client = raw.client;
  var index = 0;
  var bindings = [];

  var sql = raw.sql.replace(/\?\??/g, function (match) {
    var value = values[index++];

    if (value && typeof value.toSQL === 'function') {
      var bindingSQL = value.toSQL();
      bindings = bindings.concat(bindingSQL.bindings);
      return bindingSQL.sql;
    }

    if (match === '??') {
      return client.wrapIdentifier(value);
    }
    bindings.push(value);
    return '?';
  });

  if (expectedBindings !== index) {
    throw new Error('Expected ' + expectedBindings + ' bindings, saw ' + index);
  }

  return {
    method: 'raw',
    sql: sql,
    bindings: bindings
  };
}
function replaceKeyBindings(raw) {
  const values = raw.bindings;
  const client = raw.client;
  var bindings = [];
  const regex = /\\?(:(\w+):(?=::)|:(\w+):(?!:)|:(\w+))/g;

  const sql = raw.sql.replace(regex, function (match, p1, p2, p3, p4) {
    if (match !== p1) {
      return p1;
    }

    const part = p2 || p3 || p4;
    const key = match.trim();
    const isIdentifier = key[key.length - 1] === ':';
    const value = values[part];

    if (value === undefined) {
      if (Object.prototype.hasOwnProperty.call(values, part)) {
        formatter.bindings.push(value);
      }

      return match;
    }

    if (isIdentifier) {
      return match.replace(p1, client.wrapIdentifier(value));
    }

    bindings.push(value);
    return match.replace(p1, '?');
  });

  return {
    method: 'raw',
    sql,
    bindings: bindings,
  };
}


module.exports = Raw;
