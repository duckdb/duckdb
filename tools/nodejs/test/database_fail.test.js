var sqlite3 = require('..');
var assert = require('assert');

describe('error handling', function() {
    var db;
    before(function(done) {
        db = new sqlite3.Database(':memory:', done);
    });

    it('throw when calling Database() without new', function() {
        assert.throws(function() {
            sqlite3.Database(':memory:');
        }, (/Class constructors cannot be invoked without 'new'/));

        assert.throws(function() {
            sqlite3.Statement();
        }, (/Class constructors cannot be invoked without 'new'/));
    });

    it('should error when calling Database#get on a missing table', function(done) {
        db.get('SELECT id, txt FROM foo', function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#all prepare fail', function(done) {
        db.all('SELECT id, txt FROM foo', function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#run prepare fail', function(done) {
        db.run('SELECT id, txt FROM foo', function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#each prepare fail', function(done) {
        db.each('SELECT id, txt FROM foo', function(err, row) {
            assert.ok(false, "this should not be called");
        }, function(err, num) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#each prepare fail without completion handler', function(done) {
        db.each('SELECT id, txt FROM foo', function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#get prepare fail with param binding', function(done) {
        db.get('SELECT id, txt FROM foo WHERE id = ?', 1, function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#all prepare fail with param binding', function(done) {
        db.all('SELECT id, txt FROM foo WHERE id = ?', 1, function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#run prepare fail with param binding', function(done) {
        db.run('SELECT id, txt FROM foo WHERE id = ?', 1, function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#each prepare fail with param binding', function(done) {
        db.each('SELECT id, txt FROM foo WHERE id = ?', 1, function(err, row) {
            assert.ok(false, "this should not be called");
        }, function(err, num) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });

    it('Database#each prepare fail with param binding without completion handler', function(done) {
        db.each('SELECT id, txt FROM foo WHERE id = ?', 1, function(err, row) {
            if (err) {
                assert.equal(err.message, 'Catalog: Table with name foo does not exist!');
                assert.equal(err.errno, sqlite3.ERROR);
                //assert.equal(err.code, 'SQLITE_ERROR');
                done();
            } else {
                done(new Error('Completed query without error, but expected error'));
            }
        });
    });
});
