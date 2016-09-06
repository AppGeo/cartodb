cartodb tools
===

```bash
npm install cartodb-tools --save
```

some tools for working with cartodb, for now works only with api keys.

API is shamelessly copied from [KNEX](http://knexjs.org/) as is much of the code,
see the documentation over their for details, currently does not support table creation.

One difference is that geojson geometries are treated as such and converted to
geometries appropriate to the `the_geom` field in cartodb.


```js
var cartodb = require('cartodb-tools')('username', 'api-key');

cartodb('myTable')
  .select('foo')
  .where('bar', 'baz')
  .then(function (resp) {
    //use resp
  })
  .catch(function (err) {
    // something bad happened
  });
```

Write Stream


```js
var cartodb = require('cartodb-tools')('username', 'api-key')
cartodb.createWriteStream('table_name', opts);
// available options are `create` to create a new table
```

the query object has a few cartodb specific methods


# batch

the batch method will use the [carto batch api](https://carto.com/docs/carto-engine/sql-api/batch-queries/) method for doing the query, since this will never return results don't use it for selects, though you can if you want it's just kinda pointless

```js
cartodb('myTable')
  .update({
    foo: 'bar'
  })
  .where('bar', 'baz')
  .batch()
  .then(function (resp) {
    //use resp
  })
```

you can also use the .onSuccess or .onError method to run those queries if the first one failed or succeeded

```js
cartodb('myTable')
  .update({
    foo: 'bar'
  })
  .where('fake collumn', 'baz')
  .batch()
  .onSuccess(cartodb('errors_log').insert({
    error_message: 'NONE!',
    date: cartodb.raw('CURRENT_TIMESTAMP')
  }))
  .onError('INSERT INTO errors_log (job_id, error_message, date) VALUES (\'<%= job_id %>\', \'<%= error_message %>\', NOW())')
  .then(function (resp) {
    //use resp
  })
  ```

By default raw queries are wrapped in a transaction, use `.noTransaction()` to avoid this, useful for queries that can't be in transactions

```js
cartodb.raw('VACUUM ANALYZE').noTransaction().batch().then(function () {
  console.log('yay!');
}).catch(function () {
  console.log('no!');
});
```
