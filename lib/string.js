// from knex
// Copyright (c) 2013-2014 Tim Griesser
'use strict';

var crypto = require('crypto');
function makeValues(values) {
  var out = '';
  values.forEach(function (value, i) {
    if (i) {
      out += ',';
    } else {
      out += '(';
    }
    if (typeof value === 'boolean') {
      out += 'bool';
      return;
    }
    if (typeof value === 'number') {
      out += 'numeric';
      return;
    }
    if (typeof value === 'string') {
      out += 'text';
      return;
    }
    if (value instanceof Date) {
      out += 'timestamptz';
      return;
    }
    if (isGeojson(value)) {
      out += 'geometry';
      return;
    }
    out += 'unknown';
  });
  return out + ')';
}
function makeName (sql, values) {
  var help = '';
  if (values && values.length) {
    help = makeValues(values)
  }
  return ['_' + crypto.createHash('sha256').update(sql).update(help).digest('hex').slice(0, 16), help];
}

var debug = require('debug')('cartodb:string');
function tagRegex(tag) {
  return new RegExp('\\$' + tag + '\\$');
}
function getBase(input) {
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
exports.formatString = function formatString(item) {
  var tag = getBase(item);
  return tag + item + tag;
};
var controlRegex = /[\0\n\r\b\t\\\x1a]/g; // eslint-disable-line no-control-regex
exports.escape = function (val, timeZone) {
  if (val == null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean':
      return val ? 'true' : 'false';
    case 'number':
      return 'numeric $$' + val + '$$';
  }

  if (val instanceof Date) {
    val = exports.dateToString(val, timeZone || 'local');
  }

  if (Buffer.isBuffer(val)) {
    return exports.bufferToString(val);
  }

  if (Array.isArray(val)) {
    return exports.arrayToList(val, timeZone);
  }
  var geojson = isGeojson(val);
  if (typeof val === 'object') {
    try {
      val = JSON.stringify(val);
    } catch (e) {
      debug(e);
      val = val + '';
    }
  }

  val = val.replace(controlRegex, function (s) {
    switch (s) {
      case '\u0000':
        return '\\0';
      case '\n':
        return '\\n';
      case '\r':
        return '\\r';
      case '\b':
        return '\\b';
      case '\t':
        return '\\t';
      case '\u001a':
        return '\\Z';
      default:
        return '\\' + s;
    }
  });
  val = exports.formatString(val);
  if (geojson) {
    return 'ST_SetSRID(ST_GeomFromGeoJSON(' + val + '), 4326)';
  }
  return val;
};

exports.arrayToList = function (array, timeZone) {
  return array.map(function (v) {
    if (Array.isArray(v)) {
      return '(' + exports.arrayToList(v, timeZone) + ')';
    }
    return exports.escape(v, timeZone);
  }).join(', ');
};
function wrapIdentifier(value) {
  if (value === '*') return value;

  let arrayAccessor = '';
  const arrayAccessorMatch = value.match(/(.*?)(\[[0-9]+\])/);

  if (arrayAccessorMatch) {
    value = arrayAccessorMatch[1];
    arrayAccessor = arrayAccessorMatch[2];
  }

  return `"${value.replace(/"/g, '""')}"${arrayAccessor}`;
}
const escapeArr = (sql, values) => {
  let index = 0;
  const newValues = [];
  sql = sql.replace(/\\?\?\??/g, function (match) {
    if (match === '\\?') {
      return match;
    }
    const value = values[index++];
    if (match === '??') {
      return wrapIdentifier(value);
    }
    // if (index === values.length && match !== '?') {
    //   console.log('match', match)
    //   return match;
    // }
    newValues.push(value);
    return '$' + newValues.length;
  }) + ';';
  return {
    sql: sql,
    values: newValues
  }
}
const regex = /\\?(:(\w+):(?=::)|:(\w+):(?!:)|:(\w+))/g;
const escapeObj = (sql, values) => {
  let index = 0;
  const newValues = [];
  sql = (' ' + sql).replace(regex, function (match, p1, p2, p3, p4) {
    const part = p2 || p3 || p4;
    const key = match.trim();
    const isIdentifier = key[key.length - 1] === ':';
    const value = values[part];
    if (isIdentifier) {
      return wrapIdentifier(value);
    }
    newValues.push(value);
    return '$' + newValues.length;
  }) + ';';
  return {
    sql: sql,
    values: newValues
  }
}
const makeEscape = (sql, values) => {
  if (Array.isArray(values)) {
    if (!values.length) {
      return {
        sql: sql,
        values: []
      }
    }
    return escapeArr(sql, values);
  }
  if (!Object.keys(values).length) {
    return {
      sql: sql,
      values: []
    }
    return escapeObj(sql, values);
  }
}
function escape(timezone) {
  return escapeWithTimezone;
  function escapeWithTimezone(value) {
    return exports.escape(value, timezone)
  }
}
function formatInsert(sql, values, timeZone) {
  var index = 0;
  sql = sql.replace(/\?/g, function (match) {
    index++;
    if (index === values.length && match !== '?') {
      return match;
    }
    return '$' + index;
  }) + ';';

  let names = makeName(sql);
  let name = names[0];
  let params = names[1];
  var out = [
    'PREPARE ' + name + params + ' AS',
    sql
  ];
  values.forEach(function (item) {
    out.push('EXECUTE ' + name + (item.length ? ('(' + item.map(escape(timeZone)).join(',') + ');') : ';'));
  });
  out.push('DEALLOCATE ' + name)

  return out.join('\n');
}
exports.format = function (sql, values, timeZone, method) {
  values = values == null ? [] : values;

  if (!values.length) {
    return sql;
  }
  if (method === 'insert') {
    return formatInsert(sql, values, timeZone);
  }
  const resp = makeEscape(sql, values);
  if (!resp.values.length) {
    return resp.sql;
  }
  sql = resp.sql;
  values = resp.values;
  debug(sql);
  debug(values);

  let escapedValues = (values.length ? ('(' + values.map(escape(timeZone)).join(',') + ');') : ';');
  let names = makeName(sql, values);
  let name = names[0];
  let params = names[1]
  return [
    'PREPARE ' + name + params + ' AS',
    sql,
    'EXECUTE ' + name + escapedValues,
    'DEALLOCATE ' + name
  ].join('\n');

};
var types = [
  'Point', 'MultiPoint',
  'LineString', 'MultiLineString',
  'Polygon', 'MultiPolygon',
  'GeometryCollection'
];
function isGeojson(val) {
  if (typeof val !== 'object') {
    return false;
  }
  var type = val && val.type;
  if (types.indexOf(type) > -1) {
    return true;
  }
  return false;
}
exports.dateToString = function (date, timeZone) {
  var dt = new Date(date);

  if (timeZone !== 'local') {
    var tz = convertTimezone(timeZone);

    dt.setTime(dt.getTime() + dt.getTimezoneOffset() * 60000);
    if (tz !== false) {
      dt.setTime(dt.getTime() + tz * 60000);
    }
  }

  var year = dt.getFullYear();
  var month = zeroPad(dt.getMonth() + 1, 2);
  var day = zeroPad(dt.getDate(), 2);
  var hour = zeroPad(dt.getHours(), 2);
  var minute = zeroPad(dt.getMinutes(), 2);
  var second = zeroPad(dt.getSeconds(), 2);
  var millisecond = zeroPad(dt.getMilliseconds(), 3);

  return year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second + '.' + millisecond;
};

exports.bufferToString = function bufferToString(buffer) {
  return '$$\\x' + buffer.toString('hex') + '$$';
};

function zeroPad(number, length) {
  number = number.toString();
  while (number.length < length) {
    number = '0' + number;
  }

  return number;
}

function convertTimezone(tz) {
  if (tz === 'Z') {
    return 0;
  }

  var m = tz.match(/([\+\-\s])(\d\d):?(\d\d)?/);
  if (m) {
    return (m[1] === '-' ? -1 : 1) * (parseInt(m[2], 10) + (m[3] ? parseInt(m[3], 10) : 0) / 60) * 60;
  }
  return false;
}
// function prepare(opts) {
//   var sql = opts.sql;
//   var values = opts.values;
//   debug(values);
//   var name = makeName();
//   if (opts.method === 'insert') {
//     var out = [
//       'PREPARE ' + name + ' AS',
//       sql
//     ];
//     values.forEach(function (item) {
//       out.push('EXECUTE ' + name + (item.length ? ('(' + item.map().join(',') + ');') : ';'));
//     });
//     return out.join('\n');
//   } else {
//     return [
//       'PREPARE ' + name + ' AS',
//       sql,
//       'EXECUTE ' + name + (values.length ? ('(' + values.join(',') + ');') : ';')
//     ].join('\n');
//   }
// }
