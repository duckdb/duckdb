import * as duckdb from '..';
import * as assert from 'assert';

describe('exec', function() {
    let db: duckdb.Database;
    before(function(done) {
        db = new duckdb.Database(':memory:', done);
    });

    it("doesn't crash on a syntax error", function(done) {
        db.exec("syntax error", function(err: null | duckdb.DuckDbError) {
            assert.notEqual(err, null, "Expected an error")
            assert.equal(err?.errorType, 'Parser');
            assert.ok(err?.message.startsWith('Parser Error: syntax error at or near "syntax"'))
            done();
        });
    });
});
