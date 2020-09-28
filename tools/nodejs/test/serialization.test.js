var sqlite3 = require('..');
var assert = require('assert');


describe('serialize() and parallelize()', function() {
    var db;
    before(function(done) { db = new sqlite3.Database(':memory:', done); });

    var inserted1 = 0;
    var inserted2 = 0;
    var retrieved = 0;

    var count = 1000;

    it('should toggle', function(done) {
        db.serialize();
        db.run("CREATE TABLE foo (txt text, num int, flt float, blb blob)");
        db.parallelize(done);
    });

    it('should insert rows', function() {
        var stmt1 = db.prepare("INSERT INTO foo VALUES(?, ?, ?, ?)");
        var stmt2 = db.prepare("INSERT INTO foo VALUES(?, ?, ?, ?)");
        for (var i = 0; i < count; i++) {
            // Interleaved inserts with two statements.
            stmt1.run('String ' + i, i, i * Math.PI, function(err) {
                if (err) throw err;
                inserted1++;
            });
            i++;
            stmt2.run('String ' + i, i, i * Math.PI, function(err) {
                if (err) throw err;
                inserted2++;
            });
        }
        stmt1.finalize();
        stmt2.finalize();
    });

    it('should have inserted all the rows after synchronizing with serialize()', function(done) {
        db.serialize();
        db.all("SELECT txt, num, flt, blb FROM foo ORDER BY num", function(err, rows) {
            if (err) throw err;
            for (var i = 0; i < rows.length; i++) {
                assert.equal(rows[i].txt, 'String ' + i);
                assert.equal(rows[i].num, i);
                assert.equal(rows[i].flt, i * Math.PI);
                assert.equal(rows[i].blb, null);
                retrieved++;
            }

            assert.equal(count, inserted1 + inserted2, "Didn't insert all rows");
            assert.equal(count, retrieved, "Didn't retrieve all rows");
            done();
        });
    });

    after(function(done) { db.close(done); });
});

describe('serialize(fn)', function() {
    var db;
    before(function(done) { db = new sqlite3.Database(':memory:', done); });

    var inserted = 0;
    var retrieved = 0;

    var count = 1000;

    it('should call the callback', function(done) {
        db.serialize(function() {
            db.run("CREATE TABLE foo (txt text, num int, flt float, blb blob)");

            var stmt = db.prepare("INSERT INTO foo VALUES(?, ?, ?, ?)");
            for (var i = 0; i < count; i++) {
                stmt.run('String ' + i, i, i * Math.PI, function(err) {
                    if (err) throw err;
                    inserted++;
                });
            }
            stmt.finalize();

            db.all("SELECT txt, num, flt, blb FROM foo ORDER BY num", function(err, rows) {
                if (err) throw err;
                for (var i = 0; i < rows.length; i++) {
                    assert.equal(rows[i].txt, 'String ' + i);
                    assert.equal(rows[i].num, i);
                    assert.equal(rows[i].flt, i * Math.PI);
                    assert.equal(rows[i].blb, null);
                    retrieved++;
                }
                done();
            });
        });
    });


    it('should have inserted and retrieved all rows', function() {
        assert.equal(count, inserted, "Didn't insert all rows");
        assert.equal(count, retrieved, "Didn't retrieve all rows");
    });

    after(function(done) { db.close(done); });
});
