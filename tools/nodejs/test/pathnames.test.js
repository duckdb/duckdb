var sqlite3 = require("..");
var assert = require("assert");

describe("pathname search support", function () {
  let db;
  describe("without search paths", () => {
    before((done) => {
      db = new sqlite3.Database(":memory:", done);
    });

    it("supports a full path", function (done) {
      db.prepare('select * from "test/support/prepare.csv"').all(
        (err, result) => {
          assert(err === null);
          assert(result.length === 5000);
          done();
        }
      );
    });

    it("don't not support a partial path", function (done) {
      db.prepare('select * from "prepare.csv"').all((err, result) => {
        assert(err.code === "DUCKDB_NODEJS_ERROR");
        assert(err.errno === -1);
        assert(result == null);
        done();
      });
    });
  });

  describe("with search paths", () => {
    before((done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.prepare("SET FILE_SEARCH_PATH='test/support'").run(done);
      });
    });

    it("supports a full path", function (done) {
      db.prepare('select * from "test/support/prepare.csv"').all(
        (err, result) => {
          assert(err === null);
          assert(result.length === 5000);
          done();
        }
      );
    });

    it("supports a partial path", function (done) {
      db.prepare('select * from "prepare.csv"').all((err, result) => {
        assert(err === null);
        assert(result.length === 5000);
        done();
      });
    });
  });

  describe("with multiple search paths", () => {
    before((done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.prepare("SET FILE_SEARCH_PATH='test/support'").run(done);
      });
    });

    it("supports a full path", function (done) {
      db.prepare('select * from "test/support/prepare.csv"').all(
        (err, result) => {
          assert(err === null);
          assert(result.length === 5000);
          done();
        }
      );
    });

    it("supports a partial path", function (done) {
      db.prepare('select * from "prepare.csv"').all((err, result) => {
        assert(err === null);
        assert(result.length === 5000);
        done();
      });
    });
  });
});
