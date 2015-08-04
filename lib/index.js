'use strict';
var debug = require('debug')('cartodb');
var qs = require('querystringparser');
var url = require('url');
var https = require('https');
var stream = require('readable-stream');
var PassThrough = stream.PassThrough;
var Transform = stream.Transform;
var Writable = stream.Writable;
var JSONStream = require('jsonstream2a');
var methods = require('./methods');
var Builder = require('./builder');
var Promise = require('bluebird');
var duplexify = require('duplexify').obj;
module.exports = function (user, key) {
  var client = new CartoDB(user, key);
  function cartodb(tableName) {
    var qb = client.queryBuilder();
    return tableName ? qb.table(tableName) : qb;
  }
  cartodb.raw = function raw() {
    return client.raw.apply(client, arguments);
  };
  cartodb.createWriteStream = function (table, opts) {
    opts = opts || {};
    var created = !opts.create;
    var queue = [];
    var max = 50;
    function mabyeCreate(chunk) {
      if (created) {
        return Promise.resolve(true);
      }
      return cartodb.schema.createTable(table, function (table) {
        Object.keys(chunk.properties).forEach(function (key) {
          switch(typeof chunk.properties[key]) {
            case 'number':
              return table.float(key);
            case 'boolean':
              return table.bool(key);
            default:
              if (chunk.properties[key] instanceof Date) {
                return table.timestamp(key, true);
              }
              return table.text(key);
          }
        });
      }).then(function (a) {
        created = true;
        return a;
      });
    }
    var transform = new Transform({
      objectMode: true,
      transform: function (chunk, _, next) {
        var self = this;
        mabyeCreate(chunk).then(function () {
          queue.push(fixGeoJSON(chunk));
          if (queue.length >= max) {
            var currentQueue = queue;
            queue = [];
            self.push(currentQueue);
            next();
          } else {
            next();
          }
        }).catch(next);
      },
      flush: function (done) {
        if (queue.length) {
          this.push(queue);
        }
        done();
      }
    });
    var writable = new Writable({
      objectMode: true,
      write: function (chunk, _, next) {
        cartodb(table).insert(chunk).exec(function (err) {
          next(err);
        });
      }
    });
    transform.pipe(writable);
    return duplexify(transform, writable);
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
CartoDB.prototype.schemaCompiler = function schemaCompiler(builder) {
  return new this.SchemaCompiler(this, builder);
};

CartoDB.prototype.TableBuilder = require('./schema/tablebuilder');
CartoDB.prototype.tableBuilder = function tableBuilder(type, tableName, fn) {
  return new this.TableBuilder(this, type, tableName, fn);
};

CartoDB.prototype.TableCompiler = require('./schema/tablecompiler');
CartoDB.prototype.tableCompiler = function tableCompiler(tableBuilder) {
  return new this.TableCompiler(this, tableBuilder);
};

CartoDB.prototype.ColumnBuilder = require('./schema/columnbuilder');
CartoDB.prototype.columnBuilder = function columnBuilder(tableBuilder, type, args) {
  return new this.ColumnBuilder(this, tableBuilder, type, args);
};

CartoDB.prototype.ColumnCompiler = require('./schema/columncompiler');
CartoDB.prototype.columnCompiler = function columnCompiler(tableBuilder, columnBuilder) {
  return new this.ColumnCompiler(this, tableBuilder, columnBuilder);
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
function fixGeoJSON(chunk) {
  var out = {};
  Object.keys(chunk.properties).forEach(function (key) {
    out[key] = chunk.properties[key];
  });
  out.the_geom = chunk.geometry;
  return out;
}
