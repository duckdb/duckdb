import * as duckdb from "..";
import {TableData} from "..";
import * as assert from 'assert';

describe('UDFs', function() {
    describe('arity', function() {
        let db: duckdb.Database;
        before(function(done) {
            db = new duckdb.Database(':memory:', done);
        });

        it('0ary int', function(done) {
            db.register_udf("udf", "integer", () => 42);
            db.all("select udf() v", function(err: Error | null, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 42);
            });
            db.unregister_udf("udf", done);
        });

        it('0ary double', function(done) {
            db.register_udf("udf", "double", () => 4.2);
            db.all("select udf() v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 4.2);
            });
            db.unregister_udf("udf", done);
        });

        it('0ary string', function(done) {
            db.register_udf("udf", "varchar", () => 'hello');
            db.all("select udf() v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 'hello');
            });
            db.unregister_udf("udf", done);
        });

        it('0ary non-inlined string', function(done) {
            db.register_udf("udf", "varchar", () => 'this string is over 12 bytes');
            db.all("select udf() v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 'this string is over 12 bytes');
            });
            db.unregister_udf("udf", done);
        });

        it('0ary int null', function(done) {
            db.register_udf("udf", "integer", () => undefined);
            db.all("select udf() v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, undefined);
            });
            db.unregister_udf("udf", done);
        });


        it('0ary string null', function(done) {
            db.register_udf("udf", "varchar", () => undefined);
            db.all("select udf() v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, undefined);
            });
            db.unregister_udf("udf", done);
        });


        it('unary int', function(done) {
            db.register_udf("udf", "integer", (x) => x+1);
            db.all("select udf(42) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 43);
            });
            db.unregister_udf("udf", done);
        });

        it('unary double', function(done) {
            db.register_udf("udf", "double", (x) => x);
            db.all("select udf(4.2::double) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 4.2);
            });
            db.unregister_udf("udf", done);
        });

        it('unary int null', function(done) {
            db.register_udf("udf", "integer", (x) => undefined);
            db.all("select udf(42) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, undefined);
            });
            db.unregister_udf("udf", done);
        });


        it('unary double null', function(done) {
            db.register_udf("udf", "double", (x) => undefined);
            db.all("select udf(4.2::double) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, undefined);
            });
            db.unregister_udf("udf", done);
        });


        it('unary string', function(done) {
            db.register_udf("udf", "varchar", (x) => 'hello ' + x);
            db.all("select udf('world') v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 'hello world');
            });
            db.unregister_udf("udf", done);
        });

        it('unary string null', function(done) {
            db.register_udf("udf", "varchar", (x) => undefined);
            db.all("select udf('world') v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, undefined);
            });
            db.unregister_udf("udf", done);
        });

        it('binary int', function(done) {
            db.register_udf("udf", "integer", (x, y) => x + y);
            db.all("select udf(40, 2) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 42);
            });
            db.unregister_udf("udf", done);
        });

        it('binary string', function(done) {
            db.register_udf("udf", "varchar", (x, y) => x + ' ' + y);
            db.all("select udf('hello', 'world') v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 'hello world');
            });
            db.unregister_udf("udf", done);
        });

        it('ternary int', function(done) {
            db.register_udf("udf", "integer", (x, y, z) => x + y + z);
            db.all("select udf(21, 20, 1) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 42);
            });
            db.unregister_udf("udf", done);
        });

        it('unary larger series', function(done) {
            db.register_udf("udf", "integer", (x) => 1);
            db.all("select sum(udf(range::double)) v from range(10000)", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 10000);
            });
            db.unregister_udf("udf", done);
        });
    });

    describe('types', function() {
        var db: duckdb.Database;
        before(function(done) {
            db = new duckdb.Database(':memory:', done);
        });

        it('tinyint', function(done) {
            db.register_udf("udf", "integer", (x) => x+1);
            db.all("select udf(42::tinyint) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 43);
            });
            db.unregister_udf("udf", done);
        });

        it('smallint', function(done) {
            db.register_udf("udf", "integer", (x) => x+1);
            db.all("select udf(42::smallint) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 43);
            });
            db.unregister_udf("udf", done);
        });

        it('int', function(done) {
            db.register_udf("udf", "integer", (x) => x+1);
            db.all("select udf(42::integer) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, 43);
            });
            db.unregister_udf("udf", done);
        });

        it('timestamp', function(done) {
            db.register_udf("udf", "timestamp", (x) => x);
            db.all("select udf(timestamp '1992-09-20 11:30:00') v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
            });
            db.unregister_udf("udf", done);
        });

        it('struct', function(done) {
            db.register_udf("udf", "integer", a => {
                return (a.x == null ? -100 : a.x);
            });
            db.all("SELECT min(udf({'x': (case when v % 2 = 0 then v else null end)::INTEGER, 'y': 42}))::INTEGER as foo FROM generate_series(1, 10000) as t(v)", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].foo, -100);
            });
            db.unregister_udf("udf", done);
        });

        it('structnestednull', function(done) {
            db.register_udf("udf", "integer", a => {
                return (a.x == null ? -100 : a.x.y);
            });
            db.all("SELECT min(udf({'x': (case when v % 2 = 0 then {'y': v::INTEGER } else null end), 'z': 42}))::INTEGER as foo FROM generate_series(1, 10000) as t(v)", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].foo, -100);
            });
            db.unregister_udf("udf", done);
        });

        it('blob', function(done) {
            db.register_udf("udf", "varchar", (buf: Buffer) => buf.toString("hex"));
            db.all("select udf('\\xAA\\xAB\\xAC'::BLOB) v", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                assert.equal(rows[0].v, "aaabac");
            });
            db.unregister_udf("udf", done);
        });
    });
});
