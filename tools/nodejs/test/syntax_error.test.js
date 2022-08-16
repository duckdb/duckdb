var duckdb = require('..');
var assert = require('assert');

describe('exec', function() {
    var db;
    before(function(done) {
        db = new duckdb.Database(':memory:', done);
    });

    it("doesn't crash on a syntax error", function(done) {
        db.exec("syntax error", function(err) {
            assert.notEqual(err, null, "Expected an error")
            done();
        });
    });
});
