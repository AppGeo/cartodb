'use strict';
var debug = require('debug')('cartodb');
var qs = require('querystringparser');
var url = require('url');
var https = require('https');
var PassThrough = require('stream').PassThrough;
var JSONStream = require('jsonstream2a');
var stringUtil = require('./string');
var methods = require('./methods');
var Builder = require('./builder');

module.exports = function (user, key) {
  var client = new CartoDB(user, key);
  function cartodb(tableName) {
    var qb = client.queryBuilder();
    return tableName ? qb.table(tableName) : qb;
  }
  cartodb.raw = function raw() {
    return client.raw.apply(client, arguments);
  };
  methods.forEach(function (method) {
    cartodb[method] = function () {
      var builder = client.queryBuilder();
      return builder[method].apply(builder, arguments);
    };
  });
  Object.defineProperties(cartodb, {

    schema: {
      get: function get() {
        return client.schemaBuilder();
      }
    }
  });
  return cartodb;
};

function CartoDB(user, key) {
  this.url = 'https://' + user + '.cartodb.com/api/v2/sql';
  this.key = key;
}
CartoDB.prototype.request = function (sql) {
  var out = new PassThrough();
  var query = qs.stringify({
    api_key: this.key,
    q: sql
  });
  var opts = url.parse(this.url);
  opts.method = 'POST';
  opts.headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Content-Length': query.length
  };
  var req = https.request(opts, function (resp) {
    out.emit('headers', resp.headers);
    out.emit('code', resp.statusCode);
    debug('code: ' + resp.statusCode);
    resp.on('error', function (e) {
      out.emit('error', e);
    });
    resp.pipe(out);
  });
  req.on('error', function (e) {
    out.emit('error', e);
  });
  req.write(query);
  req.end();
  return out;
};
CartoDB.prototype.createReadStream = function (sql) {
  var out = JSONStream.parse('rows.*');
  //var out = new PassThrough();
  this.request(sql).on('error', function (e) {
    out.emit('error', e);
  }).on('headers', function (e) {
    out.emit('headers', e);
  }).on('code', function (e) {
    if (e > 299) {
      var data = [];
      this.on('data', function (d) {
        data.push(d);
      }).on('finish', function () {
        var err = Buffer.concat(data).toString();
        debug(err);
        out.emit('error', new Error(err));
      });
    } else {
      debug('pipeing');
      this.pipe(out);
    }
    out.emit('code', e);
  });
  return out;
};
CartoDB.prototype.query = function (sql, callback) {
  var out = [];
  this.createReadStream(sql).on('data', function (d) {
    out.push(d);
  }).on('error', callback).on('finish', function () {
  //  console.log('done')
    callback(null, out);
  });
};
CartoDB.prototype.exec = function (sql, cb) {
  debug(sql);
  if (typeof cb === 'function') {
    return this.query(sql, cb);
  } else {
    return this.createReadStream(sql);
  }
};
CartoDB.prototype.QueryCompiler = require('./compiler');
CartoDB.prototype.queryCompiler = function queryCompiler(builder) {
  return new this.QueryCompiler(this, builder);
};
CartoDB.prototype.QueryBuilder = require('./builder');
CartoDB.prototype.queryBuilder = function queryBuilder() {
  return new this.QueryBuilder(this);
};
CartoDB.prototype.Raw = require('./raw');
CartoDB.prototype.raw = function _raw() {
  var raw = new this.Raw(this);
  return raw.set.apply(raw, arguments);
};

CartoDB.prototype.SchemaBuilder = require('./schema/builder');
CartoDB.prototype.schemaBuilder = function schemaBuilder() {
  return new this.SchemaBuilder(this);
};

CartoDB.prototype.SchemaCompiler = require('./schema/compiler');
CartoDB.prototype.schemaCompiler = function schemaCompiler() {
  return new this.SchemaCompiler(this);
};

CartoDB.prototype.TableBuilder = require('./schema/tablebuilder');
CartoDB.prototype.tableBuilder = function tableBuilder() {
  return new this.TableBuilder(this);
};

CartoDB.prototype.TableCompiler = require('./schema/tablecompiler');
CartoDB.prototype.tableCompiler = function tableCompiler() {
  return new this.TableCompiler(this);
};

CartoDB.prototype.ColumnBuilder = require('./schema/columnbuilder');
CartoDB.prototype.columnBuilder = function columnBuilder() {
  return new this.ColumnBuilder(this);
};

CartoDB.prototype.ColumnCompiler = require('./schema/columncompiler');
CartoDB.prototype.columnCompiler = function columnCompiler() {
  return new this.ColumnCompiler(this);
};

CartoDB.prototype.Formatter = require('./formatter');
CartoDB.prototype.formatter = function formatter() {
  return new this.Formatter(this);
};

CartoDB.prototype.wrapIdentifier = function wrapIdentifier(value) {
  if (value === '*') {
    return value;
  }
  var matched = value.match(/(.*?)(\[[0-9]\])/);
  if (matched) {
    return this.wrapIdentifier(matched[1]) + matched[2];
  }
  return '"' + value.replace(/"/g, '""') + '"';
};

methods.forEach(function (method) {
  CartoDB.prototype[method] = function () {
      var builder = new Builder(this);
      return builder[method].apply(builder, arguments);
    };
});
