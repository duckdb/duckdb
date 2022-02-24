var duckdb = require('..');
var assert = require('assert');

describe('UDFs', function() {
    var db;

    before(function(done) {
        db = new duckdb.Database(':memory:', done);
    });

    it('0ary int', function(done) {
        db.register("udf", "integer", () => 42);
        db.all("select udf() v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 42);
        });
        db.unregister("udf", done);
    });

    it('0ary double', function(done) {
        db.register("udf", "double", () => 4.2);
        db.all("select udf() v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 4.2);
        });
        db.unregister("udf", done);
    });

    it('0ary string', function(done) {
        db.register("udf", "varchar", () => 'hello');
        db.all("select udf() v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 'hello');
        });
        db.unregister("udf", done);
    });

    it('0ary int null', function(done) {
        db.register("udf", "integer", () => undefined);
        db.all("select udf() v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, undefined);
        });
        db.unregister("udf", done);
    });


    it('0ary string null', function(done) {
        db.register("udf", "varchar", () => undefined);
        db.all("select udf() v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, undefined);
        });
        db.unregister("udf", done);
    });


    it('unary int', function(done) {
        db.register("udf", "integer", (x) => x+1);
        db.all("select udf(42) v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 43);
        });
        db.unregister("udf", done);
    });

    it('unary double', function(done) {
        db.register("udf", "double", (x) => x);
        db.all("select udf(4.2::double) v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 4.2);
        });
        db.unregister("udf", done);
    });

    it('unary int null', function(done) {
        db.register("udf", "integer", (x) => undefined);
        db.all("select udf(42) v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, undefined);
        });
        db.unregister("udf", done);
    });


    it('unary double null', function(done) {
        db.register("udf", "double", (x) => undefined);
        db.all("select udf(4.2::double) v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, undefined);
        });
        db.unregister("udf", done);
    });


    it('unary string', function(done) {
        db.register("udf", "varchar", (x) => 'hello ' + x);
        db.all("select udf('world') v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 'hello world');
        });
        db.unregister("udf", done);
    });

    it('unary string null', function(done) {
        db.register("udf", "varchar", (x) => undefined);
        db.all("select udf('world') v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, undefined);
        });
        db.unregister("udf", done);
    });

    it('binary int', function(done) {
        db.register("udf", "integer", (x, y) => x + y);
        db.all("select udf(40, 2) v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 42);
        });
        db.unregister("udf", done);
    });

    it('binary string', function(done) {
        db.register("udf", "varchar", (x, y) => x + ' ' + y);
        db.all("select udf('hello', 'world') v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 'hello world');
        });
        db.unregister("udf", done);
    });

    it('ternary int', function(done) {
        db.register("udf", "integer", (x, y, z) => x + y + z);
        db.all("select udf(21, 20, 1) v", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 42);
        });
        db.unregister("udf", done);
    });

    it('unary larger series', function(done) {
        db.register("udf", "integer", (x) => 1);
        db.all("select sum(udf(range::double)) v from range(10000)", function(err, rows) {
            if (err) throw err;
            assert.equal(rows[0].v, 10000);
        });
        db.unregister("udf", done);
    });
});
