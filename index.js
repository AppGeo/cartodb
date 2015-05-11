'use strict';
var debug = require('debug')('cartodb');
var qs = require('querystringparser');
var url = require('url');
var https = require('https');
var PassThrough = require('stream').PassThrough;
var JSONStream = require('jsonstream2a');
var createSQL = require('create-sql');
var crypto = require('crypto');

function makeName () {
  return '_' + crypto.randomBytes(8).toString('hex');
}

module.exports = CartoDB;

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
CartoDB.prototype.exec = function (sql, values, cb) {
  if (typeof sql === 'object') {
    cb = values;
  } else {
    sql = {
      sql: sql,
      values: values
    };
  }
  var outSql = prepare(sql);
  debug(outSql);
  if (typeof cb === 'function') {
    return this.query(outSql, cb);
  } else {
    return this.createReadStream(outSql);
  }
};

function prepare(opts) {
  var sql = opts.sql;
  var values = opts.values;
  var name = makeName();
  if (opts.insert) {
    var out = [
      'PREPARE ' + name + ' AS',
      sql,
      'BEGIN;'
    ];
    values.forEach(function (item) {
      out.push('EXECUTE ' + name + (item.length ? ('(' + item.join(',') + ');') : ';'));
    });
    out.push('COMMIT;');
    return out.join('\n');
  } else {
    return [
      'PREPARE ' + name + ' AS',
      sql,
      'EXECUTE ' + name + (values.length ? ('(' + values.join(',') + ');') : ';')
    ].join('\n');
  }
}

Object.defineProperty(CartoDB.prototype, 'select', {
  enumerable: true,
  configurable: false,
  get: function () {
    return new Select(this);
  }
});
function Select(instance) {
  this._instance = instance;
  this._table = null;
  this._rows = ['*'];
  this._where = null;
}

Select.prototype.from = function (table) {
  this._table = table;
  return this;
};

Select.prototype.columns = function (ids) {
  this._rows = Array.isArray(ids) ? ids : [ids];
  return this;
};

Select.prototype.where = function (where) {
  this._where = where;
  return this;
};
Select.prototype._makeSQL = function () {
  return createSQL.insert(this._table, this._rows, this._where);
  return this;
};
Select.prototype.exec = function (cb) {
  if (!this._table || !this._where) {
    throw new TypeError('missing required value');
  }
  return this._instance.exec(this._makeSQL(), cb);
};

Object.defineProperty(CartoDB.prototype, 'insert', {
  enumerable: true,
  configurable: false,
  get: function () {
    return new Insert(this);
  }
});
function Insert(instance) {
  this._instance = instance;
  this._table = null;
  this._values = [];
}

Insert.prototype.into = function (table) {
  this._table = table;
  return this;
};

Insert.prototype.values = function (ids) {
  this._values = this._values.concat(Array.isArray(ids) ? ids : [ids]);
  return this;
};

Insert.prototype._makeSQL = function () {
  var out = createSQL.insert(this._table, this._values);
  out.insert = true;
  return out;
};
Insert.prototype.exec = function (cb) {
  if (!this._table || !this._values.length) {
    throw new TypeError('missing required value');
  }
  return this._instance.exec(this._makeSQL(), cb);
};
Object.defineProperty(CartoDB.prototype, 'update', {
  enumerable: true,
  configurable: false,
  get: function () {
    return new Update(this);
  }
});
function Update(instance) {
  this._instance = instance;
  this._table = null;
  this._values = null;
  this._where = null;
}

Update.prototype.from = function (table) {
  this._table = table;
  return this;
};

Update.prototype.values = function (values) {
  this._values = values;
  return this;
};

Update.prototype.where = function (where) {
  this._where = where;
  return this;
};
Update.prototype._makeSQL = function () {
  return createSQL.insert(this._table, this._where, this._values);
};
Update.prototype.exec = function (cb) {
  if (!this._table || !this._values) {
    throw new TypeError('missing required value');
  }
  return this._instance.exec(this._makeSQL(), cb);
};
Object.defineProperty(CartoDB.prototype, 'delete', {
  enumerable: true,
  configurable: false,
  get: function () {
    return new Delete(this);
  }
});
function Delete(instance) {
  this._instance = instance;
  this._table = null;
  this._where = null;
}

Delete.prototype.from = function (table) {
  this._table = table;
  return this;
};

Delete.prototype.where = function (where) {
  this._where = where;
  return this;
};

Delete.prototype._makeSQL = function () {
  return createSQL.delete(this._table, this._where);
};

Delete.prototype.exec = function (cb) {
  if (!this._table) {
    throw new TypeError('missing required value');
  }
  return this._instance.exec(this._makeSQL(), cb);
};
