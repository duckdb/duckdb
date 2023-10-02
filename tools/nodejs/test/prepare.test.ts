import * as sqlite3 from '..';
import * as assert from 'assert';
import {DuckDbError, RowData, Statement, TableData} from "..";

describe('prepare', function() {
    describe('invalid SQL', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:', done); });

        var stmt;
        it('should fail preparing a statement with invalid SQL', function(done) {
            stmt = db.prepare('CRATE TALE foo text bar)', function(err: null | DuckDbError, statement: Statement) {
                if (err && err.errno == sqlite3.ERROR /*&&
                    err.message === 'Parser: syntax error at or near "CRATE' */) {
                    done();
                }
                else throw err;
            });
        });

        after(function(done) { db.close(done); });
    });

    describe('simple prepared statement', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:', done); });

        it('should prepare, run and finalize the statement', function(done) {
            db.prepare("CREATE TABLE foo (bar text)")
                .run()
                .finalize(done);
        });

        after(function(done) { db.close(done); });
    });

    describe('inserting and retrieving rows', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:', done); });

        var inserted = 0;
        var retrieved = 0;

        // We insert and retrieve that many rows.
        var count = 1000;

        it('should create the table', function(done) {
            db.prepare("CREATE TABLE foo (txt text, num int, flt double, blb blob)").run().finalize(done);
        });

        it('should insert ' + count + ' rows', function(done) {
            for (var i = 0; i < count; i++) {
                db.prepare("INSERT INTO foo VALUES(?, ?, ?, ?)").run(
                    'String ' + i,
                    i,
                    i * Math.PI,
                     null,
                    function(err: null | Error) {
                        if (err) throw err;
                        inserted++;
                    }
                ).finalize(function(err) {
                    if (err) throw err;
                    if (inserted == count) done();
                });
            }
        });

        it('should prepare a statement and return values again', function(done) {
            var stmt = db.prepare("SELECT txt, num, flt, blb FROM foo ORDER BY num", function(err: null | Error) {
                if (err) throw err;
                assert.equal(stmt.sql, 'SELECT txt, num, flt, blb FROM foo ORDER BY num');
            });

            stmt.each(function(err: null | Error, row: RowData) {
                if (err) throw err;
                assert.equal(row.txt, 'String ' + retrieved);
                assert.equal(row.num, retrieved);
                assert.equal(row.flt, retrieved * Math.PI);
                assert.equal(row.blb, null);
                retrieved++;

            });

            stmt.finalize(done);
        });

        it('should have retrieved ' + (count) + ' rows', function() {
            assert.equal(count, retrieved, "Didn't retrieve all rows");
        });


/* // get() is an abomination and should be killed
        it('should prepare a statement and run it ' + (count + 5) + ' times', function(done) {
            var stmt = db.prepare("SELECT txt, num, flt, blb FROM foo ORDER BY num", function(err) {
                if (err) throw err;
                assert.equal(stmt.sql, 'SELECT txt, num, flt, blb FROM foo ORDER BY num');
            });

            for (var i = 0; i < count + 5; i++) (function(i) {
                stmt.get(function(err, row) {
                    if (err) throw err;

                    if (retrieved >= 1000) {
                        assert.equal(row, undefined);
                    } else {
                        assert.equal(row.txt, 'String ' + i);
                        assert.equal(row.num, i);
                        assert.equal(row.flt, i * Math.PI);
                        assert.equal(row.blb, null);
                    }
                    retrieved++;
                });
            })(i);

            stmt.finalize(done);
        });

        it('should have retrieved ' + (count + 5) + ' rows', function() {
            assert.equal(count + 5, retrieved, "Didn't retrieve all rows");
        });
*/

        after(function(done) { db.close(done); });
    });


    describe('inserting with accidental undefined', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:', done); });

        var inserted = 0;
        var retrieved = 0;

        it('should create the table', function(done) {
            db.prepare("CREATE TABLE foo (num int)").run().finalize(done);
        });

        it('should insert two rows', function(done) {
            db.prepare('INSERT INTO foo VALUES(4)').run(function(err: null | Error) {
                if (err) throw err;
                inserted++;
            }).run(undefined, function (err: null | Error) {
                // The second time we pass undefined as a parameter. This is
                // a mistake, but it should either throw an error or be ignored,
                // not silently fail to run the statement.
                if (err) {
                    // errors are fine
                };
                inserted++;
            }).finalize(function(err) {
                if (err) throw err;
                if (inserted == 2) done();
            });
        });

/*
        it('should retrieve the data', function(done) {
            var stmt = db.prepare("SELECT num FROM foo", function(err) {
                if (err) throw err;
            });

            for (var i = 0; i < 2; i++) (function(i) {
                stmt.get(function(err, row) {
                    if (err) throw err;
                    assert(row);
                    assert.equal(row.num, 4);
                    retrieved++;
                });
            })(i);

            stmt.finalize(done);
        });
        */

        it('should retrieve the data', function(done) {
                    var stmt = db.prepare("SELECT num FROM foo", function(err: null | Error) {
                        if (err) throw err;
                    });

                    stmt.each(function(err: null | Error, row: RowData) {
                        if (err) throw err;
                        assert.ok(row);
                        assert.equal(row.num, 4);
                        retrieved++;
                    });

                    stmt.finalize(done);
                });

        it('should have retrieved two rows', function() {
            assert.equal(2, retrieved, "Didn't retrieve all rows");
        });

        after(function(done) { db.close(done); });
    });

/*
    describe('retrieving reset() function', function() {
        var db;
        before(function(done) { db = new sqlite3.Database(':memory:',
            function(err) {
                db.run("CREATE TEMPORARY VIEW foo AS SELECT * FROM read_csv_auto('test/support/prepare.csv')", done)
            }
            ); });

        var retrieved = 0;

        it('should retrieve the same row over and over again', function(done) {
            var stmt = db.prepare("SELECT txt, num, flt, blb FROM foo ORDER BY num");
            for (var i = 0; i < 10; i++) {
                stmt.reset();
                stmt.get(function(err, row) {
                    if (err) throw err;
                    assert.equal(row.txt, 'String 0');
                    assert.equal(row.num, 0);
                    assert.equal(row.flt, 0.0);
                    assert.equal(row.blb, null);
                    retrieved++;
                });
            }
            stmt.finalize(done);
        });

        it('should have retrieved 10 rows', function() {
            assert.equal(10, retrieved, "Didn't retrieve all rows");
        });

        after(function(done) { db.close(done); });
    });


    describe('multiple get() parameter binding', function() {
        var db;
        before(function(done) { db = new sqlite3.Database(':memory:',
            function(err) {
                db.run("CREATE TEMPORARY VIEW foo AS SELECT * FROM read_csv_auto('test/support/prepare.csv')", done)
            }
            ); });

        var retrieved = 0;

        it('should retrieve particular rows', function(done) {
            var stmt = db.prepare("SELECT txt, num, flt, blb FROM foo WHERE num = ?");

            for (var i = 0; i < 10; i++) (function(i) {
                stmt.get(i * 10 + 1, function(err, row) {
                    if (err) throw err;
                    var val = i * 10 + 1;
                    assert.equal(row.txt, 'String ' + val);
                    assert.equal(row.num, val);
                    assert.equal(row.flt, val * Math.PI);
                    assert.equal(row.blb, null);
                    retrieved++;
                });
            })(i);

            stmt.finalize(done);
        });

        it('should have retrieved 10 rows', function() {
            assert.equal(10, retrieved, "Didn't retrieve all rows");
        });

        after(function(done) { db.close(done); });
    });
    */

    describe('prepare() parameter binding', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:',
            function(err) {
                db.run("CREATE TEMPORARY VIEW foo AS SELECT * FROM read_csv_auto('test/support/prepare.csv')", done)
            }
            ); });

        var retrieved = 0;

       /* it('should retrieve particular rows', function(done) {
            db.prepare("SELECT txt, num, flt, blb FROM foo WHERE num = ? AND txt = ?", 10, 'String 10')
                .get(function(err, row) {
                    if (err) throw err;
                    assert.equal(row.txt, 'String 10');
                    assert.equal(row.num, 10);
                    assert.equal(row.flt, 10 * Math.PI);
                    assert.equal(row.blb, null);
                    retrieved++;
                })
                .finalize(done);
        }); */

        it('should retrieve particular rows', function(done) {
            db.prepare("SELECT txt, num, flt, blb FROM foo WHERE num = ? AND txt = ?")
                .each(10, 'String 10', function(err: null | Error, row: RowData) {
                    if (err) throw err;
                    assert.equal(row.txt, 'String 10');
                    assert.equal(row.num, 10);
                   //  assert.equal(row.flt, 10 * Math.PI);
                   //  assert.equal(row.blb, null);
                    retrieved++;
                })
                .finalize(done);
        });

        it('should have retrieved 1 row', function() {
            assert.equal(1, retrieved, "Didn't retrieve all rows");
        });

        after(function(done) { db.close(done); });
    });

    describe('all()', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:',
            function(err) {
                db.run("CREATE TEMPORARY VIEW foo AS SELECT * FROM read_csv_auto('test/support/prepare.csv')", done)
            }
            ); });

        var retrieved = 0;
        var count = 1000;

        it('should retrieve particular rows', function(done) {
            db.prepare("SELECT txt, num, flt, blb FROM foo WHERE num < ? ORDER BY num")
                .all(count, function(err: null | Error, rows: TableData) {
                    if (err) throw err;
                    for (var i = 0; i < rows.length; i++) {
                        assert.equal(rows[i].txt, 'String ' + i);
                        assert.equal(rows[i].num, i);
                        //assert.equal(rows[i].flt, i * Math.PI);
                        //assert.equal(rows[i].blb, null);
                        retrieved++;
                    }
                })
                .finalize(done);
        });

        it('should have retrieved all rows', function() {
            assert.equal(count, retrieved, "Didn't retrieve all rows");
        });

        after(function(done) { db.close(done); });
    });

    describe('prepare multiple statements', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:',
            function(err) {
                db.run("CREATE TABLE foo (a integer)", done)
            }
            ); });

        it('should directly execute first statements', function(done) {
            db.prepare("insert into foo values (3); insert into foo values (4); select * from foo")
                .all(function(err: null | Error, rows: TableData) {
                    if (err) throw err;
                    assert.equal(rows[0].a, 3);
                    assert.equal(rows[1].a, 4);
                })
                .finalize(done);
        });

        it('should fail in prepare, when executing the first statement', function(done) {
            let prepared = db.prepare("SELECT * FROM non_existent_table; SELECT 42", function(err: null | Error) {
				if (err) {
					done();
					return;
				}
			});
        });

        it('should fail in prepare, when preparing the first statement', function(done) {
            let prepared = db.prepare("SELCET * FROM foo; SELECT 42", function(err: null | Error) {
				if (err) {
					done();
					return;
				}
			});
        });

        after(function(done) { db.close(done); });
    });

    describe('all()', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:',
            function(err) {
                db.run("CREATE TEMPORARY VIEW foo AS SELECT * FROM read_csv_auto('test/support/prepare.csv')", done)
            }
            ); });

        it('should retrieve particular rows', function(done) {
           db.prepare("SELECT txt, num, flt, blb FROM foo WHERE num > 5000")
                .all(function(err: null | Error, rows: TableData) {
                    if (err) throw err;
                    assert.ok(rows.length === 0);
                })
                .finalize(done);
        });

        after(function(done) { db.close(done); });
    });

    describe('high concurrency', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:', done); });

        function randomString() {
            var str = '';
            var characters  = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ';

            for (var i = Math.random() * 300; i > 0; i--) {
                str += characters.charAt(Math.floor(Math.random() * characters.length))
            }
            return str;

        }

        // Generate random data.
        var data: [string, number, number, null][] = [];
        var retrieved_marks: boolean[] = [];
        var length = Math.floor(Math.random() * 1000) + 200;
        for (var i = 0; i < length; i++) {
            data.push([ randomString(), i, i * Math.random(), null ]);
            retrieved_marks.push(false);
        }
        var inserted = 0;
        var retrieved = 0;

        it('should create the table', function(done) {
            db.prepare("CREATE TABLE foo (txt text, num int, flt float, blb blob)").run().finalize(done);
        });

        it('should insert all values', function(done) {
            for (var i = 0; i < data.length; i++) {
                var stmt = db.prepare("INSERT INTO foo VALUES(?, ?, ?, ?)");
                stmt.run(data[i][0], data[i][1], data[i][2], data[i][3], function(err: null | Error) {
                    if (err) throw err;
                    inserted++;
                }).finalize(function(err) {
                    if (err) throw err;
                    if (inserted == data.length) done();
                });
            }
        });

        it('should retrieve all values', function(done) {
            db.prepare("SELECT txt, num, flt, blb FROM foo")
                .all(function(err: null | Error, rows: TableData) {
                    if (err) throw err;

                    for (var i = 0; i < rows.length; i++) {
                        assert.ok(retrieved_marks[rows[i].num] !== true);

                        assert.equal(rows[i].txt, data[rows[i].num][0]);
                        assert.equal(rows[i].num, data[rows[i].num][1]);
                        //assert.equal(rows[i].flt, data[rows[i].num][2]);
                        //assert.equal(rows[i].blb, data[rows[i].num][3]);

                        // Mark the data row as already retrieved.
                        retrieved_marks[rows[i].num] = true;
                        retrieved++;

                    }

                    assert.equal(retrieved, data.length);
                    assert.equal(retrieved, inserted);
                })
                .finalize(done);
        });

        after(function(done) { db.close(done); });
    });

/*
    describe('test Database#get()', function() {
        var db;
        before(function(done) { db = new sqlite3.Database(':memory:',
            function(err) {
                db.run("CREATE TEMPORARY VIEW foo AS SELECT * FROM read_csv_auto('test/support/prepare.csv')", done)
            }
            ); });

        var retrieved = 0;

        it('should get a row', function(done) {
            db.get("SELECT txt, num, flt, blb FROM foo WHERE num = ? AND txt = ?", 10, 'String 10', function(err, row) {
                if (err) throw err;
                assert.equal(row.txt, 'String 10');
                assert.equal(row.num, 10);
                assert.equal(row.flt, 10 * Math.PI);
                assert.equal(row.blb, null);
                retrieved++;
                done();
            });
        });

        it('should have retrieved all rows', function() {
            assert.equal(1, retrieved, "Didn't retrieve all rows");
        });

        after(function(done) { db.close(done); });
    });
*/
    describe('Database#run() and Database#all()', function() {
        var db: sqlite3.Database;
        before(function(done) { db = new sqlite3.Database(':memory:', done); });

        var inserted = 0;
        var retrieved = 0;

        // We insert and retrieve that many rows.
        var count = 1000;

        it('should create the table', function(done) {
            db.run("CREATE TABLE foo (txt text, num int, flt double, blb blob)", done);
        });

        it('should insert ' + count + ' rows', function(done) {
            for (var i = 0; i < count; i++) {
                db.run("INSERT INTO foo VALUES(?, ?, ?, ?)",
                    'String ' + i,
                    i,
                    i * Math.PI,
                     null,
                    function(err: null | Error) {
                        if (err) throw err;
                        inserted++;
                        if (inserted == count) done();
                    }
                );
            }
        });

        it('should retrieve all rows', function(done) {
            db.all("SELECT txt, num, flt, blb FROM foo ORDER BY num", function(err: null | Error, rows: TableData) {
                if (err) throw err;
                for (var i = 0; i < rows.length; i++) {
                    assert.equal(rows[i].txt, 'String ' + i);
                    assert.equal(rows[i].num, i);
                    assert.equal(rows[i].flt, i * Math.PI);
                    assert.equal(rows[i].blb, null);
                    retrieved++;
                }

                assert.equal(retrieved, count);
                assert.equal(retrieved, inserted);

                done();
            });
        });

        describe('using aggregate functions', function() {
            it("should aggregate string_agg(txt)", function (done) {
                db.all("SELECT string_agg(txt, ',') as string_agg FROM foo WHERE num < 2", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].string_agg, "String 0,String 1");
                    done(err);
                });
            });

            it("should aggregate min(flt)", function (done) {
                db.all("SELECT min(flt) as min FROM foo WHERE flt > 0", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].min, Math.PI);
                    done(err);
                });
            });
            it("should aggregate max(flt)", function (done) {
                db.all("SELECT max(flt) as max FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].max, Math.PI * 999);
                    done(err);
                });
            });
            it("should aggregate avg(flt)", function (done) {
                db.all("SELECT avg(flt) as avg FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].avg, 1569.2255304681016);
                    done(err);
                });
            });
            it("should aggregate first(flt)", function (done) {
                db.all("SELECT first(flt) as first FROM foo WHERE flt > 0", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].first, Math.PI);
                    done(err);
                });
            });
            it("should aggregate approx_count_distinct(flt)", function (done) {
                db.all("SELECT approx_count_distinct(flt) as approx_count_distinct FROM foo", function (err: null | Error, res: TableData) {
                    assert.ok(res[0].approx_count_distinct >= 950);
                    done(err);
                });
            });
            it("should aggregate sum(flt)", function (done) {
                db.all("SELECT sum(flt) as sum FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].sum, 1569225.5304681016);
                    done(err);
                });
            });


            it("should aggregate min(num)", function (done) {
                db.all("SELECT min(num) as min FROM foo WHERE num > 0", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].min, 1);
                    done(err);
                });
            });
            it("should aggregate max(num)", function (done) {
                db.all("SELECT max(num) as max FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].max, 999);
                    done(err);
                });
            });
            it("should aggregate count(num)", function (done) {
                db.all("SELECT count(num) as count FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].count, 1000);
                    done(err);
                });
            });
            it("should aggregate avg(num)", function (done) {
                db.all("SELECT avg(num) as avg FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].avg, 499.5);
                    done(err);
                });
            });
            it("should aggregate first(num)", function (done) {
                db.all("SELECT first(num) as first FROM foo WHERE num > 0", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].first, 1);
                    done(err);
                });
            });
            it("should aggregate approx_count_distinct(num)", function (done) {
                db.all("SELECT approx_count_distinct(num) as approx_count_distinct FROM foo", function (err: null | Error, res: TableData) {
                    assert.ok(res[0].approx_count_distinct >= 950);
                    done(err);
                });
            });
            it("should aggregate approx_quantile(num, 0.5)", function (done) {
                db.all("SELECT approx_quantile(num, 0.5) as approx_quantile FROM foo", function (err: null | Error, res: TableData) {
                    assert.ok(res[0].approx_quantile >= 499);
                    done(err);
                });
            });
            it("should aggregate reservoir_quantile(num, 0.5, 10)", function (done) {
                db.all("SELECT reservoir_quantile(num, 0.5, 10) as reservoir_quantile FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].reservoir_quantile, 4);
                    done(err);
                });
            });
            it("should aggregate var_samp(num)", function (done) {
                db.all("SELECT var_samp(num) as var_samp FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].var_samp, 83416.66666666667);
                    done(err);
                });
            });
            it("should aggregate kurtosis(num)", function (done) {
                db.all("SELECT kurtosis(num) as kurtosis FROM foo", function (err: null | Error, res: TableData) {
                    // The `num` column of table `foo` contains each integer from 0 to 999,999 exactly once.
                    // This is a uniform distribution. The excess kurtosis for a uniform distribution is exactly -1.2.
                    // See https://en.wikipedia.org/wiki/Kurtosis#Other_well-known_distributions
                    const expected = -1.2;
                    
                    // The calculated value can differ from the exact answer by small amounts on different platforms due
                    // to floating-point errors. This tolerance was determined experimentally.
                    const tolerance = Number.EPSILON * 10;

                    assert.ok(Math.abs(res[0].kurtosis - expected) < tolerance);
                    done(err);
                });
            });

            it("should aggregate sum(num)", function (done) {
                db.all("SELECT sum(num) as sum FROM foo", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].sum, 499500);
                    done(err);
                });
            });

            it("should aggregate product(num)", function (done) {
                db.all("SELECT product(num) as product FROM foo WHERE num < 20 AND num > 0", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].product, 121645100408832000);
                    done(err);
                });
            });


            it("should aggregate product(flt)", function (done) {
                db.all("SELECT product(flt) as product FROM foo WHERE num < 10 AND num > 0", function (err: null | Error, res: TableData) {
                    assert.equal(res[0].product, 10817125966.120956);
                    done(err);
                });
            });


        });

        after(function(done) { db.close(done); });
    });
});
