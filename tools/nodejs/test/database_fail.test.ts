import * as sqlite3 from '..';
import * as assert from 'assert';
import {DuckDbError, RowData} from "..";

describe('error handling', function() {
    var db: sqlite3.Database;
    before(function(done) {
        db = new sqlite3.Database(':memory:', done);
    });

    it('throw when calling Database() without new', function() {
        assert.throws(function() {
            // @ts-ignore
            sqlite3.Database(':memory:');
        }, (/Class constructors cannot be invoked without 'new'/));

        assert.throws(function() {
            // @ts-ignore
            sqlite3.Statement();
        }, (/Class constructors cannot be invoked without 'new'/));
    });

    it('should error when calling Database#each on a missing table', function(done) {
        db.each('SELECT id, txt FROM foo', function(err: null | DuckDbError, row: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#all prepare fail', function(done) {
        db.all('SELECT id, txt FROM foo', function(err: null | DuckDbError, row: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#run prepare fail', function(done) {
        db.run('SELECT id, txt FROM foo', function(err: null | DuckDbError, row: void) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it.skip('Database#each prepare fail', function(done) {
        db.each('SELECT id, txt FROM foo', function(err: null | DuckDbError, row: RowData) {
            assert.ok(false, "this should not be called");
        }, function(err: null | DuckDbError, num: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#each prepare fail without completion handler', function(done) {
        db.each('SELECT id, txt FROM foo', function(err: null | DuckDbError, row: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it.skip('Database#get prepare fail with param binding', function(done) {
        db.get('SELECT id, txt FROM foo WHERE id = ?', 1, function(err: null | DuckDbError, row: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#all prepare fail with param binding', function(done) {
        db.all('SELECT id, txt FROM foo WHERE id = ?', 1, function(err: null | DuckDbError, row: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#run prepare fail with param binding', function(done) {
        db.run('SELECT id, txt FROM foo WHERE id = ?', 1, function(err: null | DuckDbError, row: void) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it.skip('Database#each prepare fail with param binding', function(done) {
        db.each('SELECT id, txt FROM foo WHERE id = ?', 1, function(err: null | DuckDbError, row: RowData) {
            assert.ok(false, "this should not be called");
        }, function(err: null | DuckDbError, row: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#each prepare fail with param binding without completion handler', function(done) {
        db.each('SELECT id, txt FROM foo WHERE id = ?', 1, function(err: null | DuckDbError, row: RowData) {
            if (err) {
                assert.equal(err.message.includes('does not exist'), 1);
                assert.equal(err.errno, sqlite3.ERROR);
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });
});
