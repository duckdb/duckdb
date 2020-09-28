var sqlite3 = require('..');
var assert = require('assert');

describe('query properties', function() {
    var db;
    before(function(done) {
        db = new sqlite3.Database(':memory:');
        db.run("CREATE TABLE foo (id INT, txt TEXT)", done);
    });

/*
    it('should return the correct lastID', function(done) {
        var stmt = db.prepare("INSERT INTO foo VALUES(?, ?)");
        var j = 1;
        for (var i = 0; i < 5000; i++) {
            stmt.run(i, "demo", function(err) {
                if (err) throw err;
                // Relies on SQLite's row numbering to be gapless and starting
                // from 1.
                assert.equal(j++, this.lastID);
            });
        }
        db.wait(done);
    });
    */

    it('should return the correct changes count', function(done) {
        db.run("UPDATE foo SET id = id + 1 WHERE id % 2 = 0", function(err) {
            if (err) throw err;
           // FIXME assert.equal(2500, this.changes);
            done();
        });
    });
});
