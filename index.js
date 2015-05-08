'use strict';
var qs = require('querystringparser');
var url = require('url');
var https = require('https');
var PassThrough = require('stream').PassThrough;
var JSONStream = require('JSONStream');
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
  }
  var req = https.request(opts, function (resp) {
    out.emit('headers', resp.headers);
    out.emit('code', resp.statusCode);
    resp.on('error', function (e) {
      out.emit('error', e);
    })
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
  return this.request(sql).on('error', function (e) {
    out.emit('error', e);
  }).on('headers', function (e) {
    out.emit('headers', e);
  }).on('code', function (e) {
    out.emit('code', e);
  }).pipe(out);
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
  var outSql = prepare({
    sql: sql,
    values: values
  });
  if (typeof cb === 'function') {
    return this.query(outSql, cb);
  } else {
    return this.ce.createReadStream(outSql);
  }
};
function tagRegex(tag) {
  return new RegExp('\\$' + tag + '\\$');
}
function formatString(input) {
  var tagBase = 'cartodb';
  if (!input.match(tagRegex(tagBase))) {
    return '$' + tagBase + '$';
  }
  var i = 0;
  while(input.match(tagRegex(tagBase + i))) {
    i++;
  }
  return '$' + tagBase + i + '$';
}
function cleanType(item) {
  switch(typeof item) {
    case 'string':
      return formatString(item);
    case 'number':
      return 'numeric $$' + item + '$$';
    case 'boolean':
      return item ? 'true' : 'false';
    case 'object':
      if (Buffer.isBuffer(item)) {
        return '$$\x' + item.toString('hex') + '$$';
      }
      if (item.type === 'Feature') {
        return 'ST_GeomFromGeoJSON(' + formatString(JSON.stringify(item)) + ')';
      }
      return formatString(JSON.stringify(item));
    default:
      throw new TypeError('invalid data type: ' + typeof item);
  }
}
function prepare(opts) {
  var sql = opts.sql;
  var values = opts.values;
  var name = makeName();
  return [
    'PREPARE ' + name + '() AS',
    sql,
    'EXECUTE ' + name + '(' + values.map(cleanType).join(',') + ');'
  ].join('\n');
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
};

Select.prototype.columns = function (ids) {
  this._rows = Array.isArrays(ids) ? ids : [ids];
};

Select.prototype.where = function (where) {
  this._where = where;
};
Select.prototype._makeSQL = function () {
  return createSQL.insert(this._table, this._rows, this._where);
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
  this._values = null;
}

Insert.prototype.into = function (table) {
  this._table = table;
};

Insert.prototype.values = function (ids) {
  this._rows = Array.isArrays(ids) ? ids : [ids];
};

Insert.prototype._makeSQL = function () {
  return createSQL.insert(this._table, this._values);
};
Insert.prototype.exec = function (cb) {
  if (!this._table || !this._values) {
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
};

Update.prototype.values = function (values) {
  this._values = values;
};

Update.prototype.where = function (where) {
  this._where = where;
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
};

Delete.prototype.where = function (where) {
  this._where = where;
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
