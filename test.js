'use strict';

var test = require('tape');
var config = require('./test.config.json');
var cartodb = require('./')(config.username, config.apikey);
var TABLE_NAME = 'test_table';
test('basic', function (t) {
  t.test('clear', function (t) {
    t.plan(1);
    cartodb(TABLE_NAME).delete()
    .exec(function (err) {
      t.error(err);
      console.log('err', err && err.toString());
    });
  });
  t.test('clear check', function (t) {
    t.plan(2);
    cartodb(TABLE_NAME).count()
    .exec(function (err, resp) {
      t.error(err);
      t.deepEquals([{count: 0}], resp);
    });
  });
  t.test('insert', function (t) {
    t.plan(1);
    cartodb.insert([
      {
        ammount: 1,
        description: 'one',
        ifso: true
      },
      {
        ammount: 2,
        description: 'two',
        ifso: false,
        name: 'even'
      },
      {
        ammount: 3,
        description: 'three',
        ifso: true
      },
      {
        description: 'try to use $cartodb$ to escape',
        ifso: false,
        the_geom: {"type":"Point","coordinates":[-98.19805569,29.49655938]}
      }
    ]).into(TABLE_NAME).exec(function (err) {
      t.error(err, err && err.stack);
    });
  });
  t.test('insert check', function (t) {
    t.plan(2);
    cartodb.from(TABLE_NAME).count()
    .exec(function (err, resp) {
      t.error(err);
      t.deepEquals([{count: 4}], resp);
    });
  });
  t.test('select 1', function (t) {
    t.plan(1);
    cartodb.select('ammount').from(TABLE_NAME).where('ifso', true).then(function (resp) {
      if (!resp) {
        return t.ok(resp);
      }
      t.deepEquals(resp.sort(function (a, b) {
        return a.ammount - b.ammount;
      }), [ { ammount: 1 }, { ammount: 3 } ]);
    }).catch(function () {
      t.notOk(true);
    });
  });
  t.test('update', function (t) {
    t.plan(1);
    cartodb(TABLE_NAME).update({
      ifso: true
    }).where('name', 'even').exec(function (err) {
      t.error(err, err && err.stack);
    });
  });
  t.test('select 2', function (t) {
    t.plan(2);
    cartodb.select('ammount').from(TABLE_NAME).where('ifso', true).exec(function (err, resp) {
      t.error(err, err && err.stack);
      if (!resp) {
        return t.ok(resp);
      }
      t.deepEquals(resp.sort(function (a, b) {
        return a.ammount - b.ammount;
      }), [ { ammount: 1 }, { ammount: 2 }, { ammount: 3 } ]);
    });
  });
  t.test('check version', function (t) {
    t.plan(1);
    cartodb.raw('select version();').then(function () {
      t.ok(true);//, JSON.stringify(resp, false, 2));
    }).catch(function (e) {
      t.error(e);
    });
  });
  t.test('table info', function (t) {
    t.plan(1);
    cartodb(cartodb.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name')
    .where({
      table_name: TABLE_NAME
    }).then(function () {
      t.ok(true);//, JSON.stringify(resp, false, 2));
    }).catch(function (e) {
      t.error(e);
    });
  });
  t.test('select 2', function (t) {
    t.plan(2);
    cartodb.select('ammount').from(TABLE_NAME).whereRaw('ifso = ?', [true]).limit(1).exec(function (err, resp) {
      t.error(err, err && err.stack);
      if (!resp) {
        return t.ok(resp);
      }
      t.deepEquals(resp.sort(function (a, b) {
        return a.ammount - b.ammount;
      }), [ { ammount: 1 } ]);
    });
  });
});
