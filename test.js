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
        ifso: true,
        updated_at2: new Date()
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
  t.test('select 2', function (t) {
    t.plan(1);
    cartodb.select('ammount').from(TABLE_NAME).where('updated_at2', '<', new Date()).then(function (resp) {
      if (!resp) {
        return t.ok(resp);
      }
      t.deepEquals(resp, [  { ammount: 3 } ]);
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
  t.test('update 2', function (t) {
    t.plan(1);
    cartodb(TABLE_NAME).update({
      the_geom: {"type":"MultiPoint","coordinates":[[-98.19805569,29.49655938],[-97,28]]}
    }).where({the_geom: {"type":"Point","coordinates":[-98.19805569,29.49655938]}}).exec(function (err) {
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
    cartodb(cartodb.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name', 'data_type')
    .where({
      table_name: TABLE_NAME
    }).then(function (resp) {
      t.ok(true, JSON.stringify(resp, false, 2));
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
  t.test('create table', function (t) {
    t.plan(1);
    cartodb.schema.createTable('cartodb_tools_test_fin', function (table) {
      table.boolean('bool');
      table.text('some_text');
    }).then(function (resp) {
      t.ok(true, JSON.stringify(resp, false, 2));
    }).catch(function (e) {
      t.error(e);
    });
  });
  t.test('created table info', function (t) {
    t.plan(1);
    cartodb(cartodb.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name', 'data_type')
    .where({
      table_name: 'cartodb_tools_test_fin'
    }).then(function (resp) {
      t.equals(resp.length, 5);
    }).catch(function (e) {
      t.error(e);
    });
  });
  t.test('drop table', function (t) {
    t.plan(1);
    cartodb.schema.dropTableIfExists('cartodb_tools_test_fin').then(function (resp) {
      t.ok(true, JSON.stringify(resp, false, 2));
    }).catch(function (e) {
      t.error(e);
    });
  });
});
test('advanced', function (t) {
  t.test('control', function (t) {
    t.plan(1);
    cartodb.raw('select true').then(function (r){
      console.log(r);
      t.ok(true, 'works');
    }).catch(function (e) {
      t.error(e);
    })
  });
  t.test('new way', function (t) {
    t.plan(1);
    cartodb.raw('select true').batch().then(function (r){
      t.ok(true, 'works');
    }).catch(function (e) {
      t.error(e, e.stack);
    })
  });
  t.test('fallback', function (t) {
    t.plan(1);
    cartodb.raw('INSERT INTO errors_log (job_id, error_message, date) VALUES (\'first part!!!!\', \'no error\', NOW())')
    .onSuccess(cartodb.raw('INSERT INTO errors_log (job_id, error_message, date) VALUES (\'success part!!!!\', \'no error\', NOW())'))
    .onError(cartodb.raw('INSERT INTO errors_log (job_id, error_message, date) VALUES (\'<%= job_id %>\', \'<%= error_message %>\', NOW())')).batch().then(function (r){
      t.ok(true, 'works');
    }).catch(function (e) {
      t.error(e, e.stack);
    })
  });
})
