var sqlite3 = require('..');
var assert = require("assert");

describe("pathname search support", function () {
    let db;
    it("supports full paths", function (done) {
        db = new sqlite3.Database(":memory:", () => {
            db.prepare('select * from "test/support/prepare.csv"').all((err, result) => {
                assert(err === null);
                assert(result.length === 5000);
                done();
            });
        });
    });

    it("supports search paths", function (done) {
        db = new sqlite3.Database(":memory:", ["test/support/"], () => {
            db.prepare('select * from "prepare.csv"').all((err, result) => {
                assert(err === null);
                assert(result.length === 5000);
                done();
            });
        });
    });
});
