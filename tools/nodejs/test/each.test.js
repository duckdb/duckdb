var sqlite3 = require('..');
var assert = require('assert');

describe('each', function() {
    var db;
    before(function(done) {
        db = new sqlite3.Database('test/support/big.db', sqlite3.OPEN_READONLY, done);
    });

    it('retrieve 100,000 rows with Statement#each', function(done) {
        var total = 100000;
        var retrieved = 0;
        

        db.each('SELECT id, txt FROM foo LIMIT 0, ?', total, function(err, row) {
            if (err) throw err;
            retrieved++;
            
            if(retrieved === total) {
                assert.equal(retrieved, total, "Only retrieved " + retrieved + " out of " + total + " rows.");
                done();
            }
        });
    });

    it('Statement#each with complete callback', function(done) {
        var total = 10000;
        var retrieved = 0;

        db.each('SELECT id, txt FROM foo LIMIT 0, ?', total, function(err, row) {
            if (err) throw err;
            retrieved++;
        }, function(err, num) {
            assert.equal(retrieved, num);
            assert.equal(retrieved, total, "Only retrieved " + retrieved + " out of " + total + " rows.");
            done();
        });
    });
});
