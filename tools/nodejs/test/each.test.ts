import * as sqlite3 from '..';
import * as assert from 'assert';
import {RowData} from "..";

describe('each', function() {
    var db: sqlite3.Database;
    before(function(done) {
        db = new sqlite3.Database('test/support/big.db', done);
    });

    it('retrieve 100,000 rows with Statement#each', function(done) {
        var total = 100000;
        var retrieved = 0;
        

        db.each('SELECT id, txt FROM foo WHERE ROWID < ?', total, function(err: null | Error, row: RowData) {
            if (err) throw err;
            retrieved++;

            if(retrieved === total) {
                assert.equal(retrieved, total, "Only retrieved " + retrieved + " out of " + total + " rows.");
                done();
            }
        });
    });

    it.skip('Statement#each with complete callback', function(done) {
        var total = 10000;
        var retrieved = 0;

        db.each('SELECT id, txt FROM foo WHERE ROWID < ?', total, function(err: null | Error, row: RowData) {
            if (err) throw err;
            retrieved++;
        }, function(err: null | Error, num: RowData) {
            assert.equal(retrieved, num);
            assert.equal(retrieved, total, "Only retrieved " + retrieved + " out of " + total + " rows.");
            done();
        });
    });
});
