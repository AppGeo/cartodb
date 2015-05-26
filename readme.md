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
var cartodb = require('cartodb-tools')('username', 'api-key')
cartodb(TABLE_NAME).select('foo').where('bar', 'baz').then(function (resp) {
  //use resp
}).catch(function (err) {
  // something bad happened
});
```

Write Stream


```js
var cartodb = require('cartodb-tools')('username', 'api-key')
cartodb.createWriteStream('table_name', opts);
// available options are `create` to create a new table
```
