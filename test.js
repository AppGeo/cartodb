'use strict';
var Cartodb = require('./');
var test = require('tape');
var config = require('./test.config.json');
var cartodb = new Cartodb(config.username, config.apikey);
var TABLE_NAME = 'test_table';
test('basic', function (t) {
  t.test('clear', function (t) {
    t.plan(1);
    cartodb.delete.from(TABLE_NAME).exec(function (err) {
      t.error(err);
      console.log('err', err && err.toString());
    });
  });
  t.test('insert', function (t) {
    t.plan(1);
    cartodb.insert.into(TABLE_NAME)
    .values([
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
      }
    ]).exec(function (err) {
      t.error(err, err && err.stack);
    });
  });
});
