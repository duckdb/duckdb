# DuckDB Node Bindings

This package provides a node.js API for [DuckDB](https://github.com/cwida/duckdb), the "SQLite for Analytics". The API for this client is somewhat compliant to the SQLite node.js client for easier transition (and transition you must eventually).

Load the package and create a database object:

```js
var duckdb = require('duckdb');

var db = new duckdb.Database(':memory:'); // or a file name for a persistent DB
```

Then you can run a query:

```js
db.all('SELECT 42 AS fortytwo', function(err, res) {
  if (err) {
    throw err;
  }
  console.log(res[0].fortytwo)
});
```

Other available methods are `each`, where the callback is invoked for each row, `run` to execute a single statement without results and `exec`, which can execute several SQL commands at once but also does not return results. All those commands can work with prepared statements, taking the values for the parameters as additional arguments. For example like so:

```js
db.all('SELECT ?::INTEGER AS fortytwo, ?::STRING as hello', 42, 'Hello, World', function(err, res) {
  if (err) {
    throw err;
  }
  console.log(res[0].fortytwo)
  console.log(res[0].hello)
});
```

However, these are all shorthands for something much more elegant. A database can have multiple `Connection`s, those are created using `db.connect()`.

```js
var con = db.connect();
```

You can create multiple connections, each with their own transaction context.


`Connection` objects also contain shorthands to directly call `run()`, `all()` and `each()` with parameters and callbacks, respectively, for example:

```js
con.all('SELECT 42 AS fortytwo', function(err, res) {
  if (err) {
    throw err;
  }
  console.log(res[0].fortytwo)
});
```

From connections, you can create prepared statements (and only that) using `con.prepare()`:

```js
var stmt = con.prepare('select ?::INTEGER as fortytwo');
``` 

To execute this statement, you can call for example `all()` on the `stmt` object:

```js
stmt.all(42, function(err, res) {
  if (err) {
    throw err;
  }
  console.log(res[0].fortytwo)
});
```

You can also execute the prepared statement multiple times. This is for example useful to fill a table with data:

```js
con.run('CREATE TABLE a (i INTEGER)');
var stmt = con.prepare('INSERT INTO a VALUES (?)');
for (var i = 0; i < 10; i++) {
  stmt.run(i);
}
stmt.finalize();
con.all('SELECT * FROM a', function(err, res) {
  if (err) {
    throw err;
  }
  console.log(res)
});
```

`prepare()` can also take a callback which gets the prepared statement as an argument:

```js
var stmt = con.prepare('select ?::INTEGER as fortytwo', function(err, stmt) {
  stmt.all(42, function(err, res) {
    if (err) {
      throw err;
    }
    console.log(res[0].fortytwo)
  });
});
```

## Development

Tests are located in `tools/nodejs/test`, these tests require Mocha to run.
Along with all the other dev dependencies, this can be installed using `npm install` ran from `tools/nodejs` (this uses package.json)
Tests can then be run with `npm test`

To build the NodeJS package from source, when on Windows, requires the following extra steps:
- Set `OPENSSL_ROOT_DIR` to the root directory of an OpenSSL installation
- Supply the `STATIC_OPENSSL=1` option when executing `make`, or set `-DOPENSSL_USE_STATIC_LIBS=1` manually when calling `cmake`

