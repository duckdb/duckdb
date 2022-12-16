import * as sqlite3 from '..';
import * as assert from 'assert';
import {DuckDbError} from "..";

// FIXME each is not streaming yet

describe.skip('interrupt', function() {
    it('should interrupt queries', function(done) {
        var interrupted = false;
        var saved: DuckDbError | null = null;

        var db = new sqlite3.Database(':memory:', function() {
            db.serialize();

            var setup = 'create table t (n int);';
            for (var i = 0; i < 8; i += 1) {
                setup += 'insert into t values (' + i + ');';
            }

            db.exec(setup, function(err: null | Error) {
                if (err) {
                    return done(err);
                }

                var query = 'select last.n ' +
                    'from t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t,t as last';

                db.each(query, function(err: null | DuckDbError) {
                    if (err) {
                        saved = err;
                    } else if (!interrupted) {
                        interrupted = true;
                        db.interrupt();
                    }
                });

                db.close(function() {
                    if (saved) {
                        assert.equal(saved.message, 'SQLITE_INTERRUPT: interrupted');
                        assert.equal(saved.errno, sqlite3.INTERRUPT);
                        assert.equal(saved.code, 'SQLITE_INTERRUPT');
                        done();
                    } else {
                        done(new Error('Completed query without error, but expected error'));
                    }
                });
            });
        });
    });

    it('should throw if interrupt is called before open', function(done) {
        var db = new sqlite3.Database(':memory:');

        assert.throws(function() {
            db.interrupt();
        }, (/Database is not open/));

        db.close();
        done();
    });

    it('should throw if interrupt is called after close', function(done) {
        var db = new sqlite3.Database(':memory:');

        db.close(function() {
            assert.throws(function() {
                db.interrupt();
            }, (/Database is not open/));

            done();
        });
    });

    it('should throw if interrupt is called during close', function(done) {
        var db = new sqlite3.Database(':memory:', function() {
            db.close();
            assert.throws(function() {
                db.interrupt();
            }, (/Database is closing/));
            done();
        });
    });
});
