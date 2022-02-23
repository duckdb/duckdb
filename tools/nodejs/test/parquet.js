var sqlite3 = require('..');
var assert = require('assert');
var helper = require('./support/helper');

describe('can query parquet', function() {
    var db;

    before(function(done) {
        db = new sqlite3.Database(':memory:', done);
    });

    it('should be able to read parquet files', function(done) {
        db.run("select * from parquet_scan('../../data/parquet-testing/userdata1.parquet')", done);
    });

});
