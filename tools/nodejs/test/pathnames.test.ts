import * as sqlite3 from "..";
import * as assert from "assert";
import {DuckDbError, TableData} from "..";

describe("pathname search support", function () {
  let db: sqlite3.Database;
  describe("without search paths", () => {
    before((done) => {
      db = new sqlite3.Database(":memory:", done);
    });

    it("supports a full path", function (done) {
      db.prepare('select * from "test/support/prepare.csv"').all(
        (err: null | Error, result: TableData) => {
          assert.equal(err, null);
          assert.equal(result.length, 5000);
          done();
        }
      );
    });

    it("don't not support a partial path", function (done) {
      db.prepare('select * from "prepare.csv"').all((err: null | DuckDbError, result: TableData) => {
        assert.ok(err);
        assert.equal(err.code, "DUCKDB_NODEJS_ERROR");
        assert.equal(err.errno, -1);
        assert.equal(result, null);
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
        (err: null | Error, result: TableData) => {
          assert.equal(err, null);
          assert.equal(result.length, 5000);
          done();
        }
      );
    });

    it("supports a partial path", function (done) {
      db.prepare('select * from "prepare.csv"').all((err: null | Error, result: TableData) => {
        assert.equal(err, null);
        assert.equal(result.length, 5000);
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
        (err: null | Error, result: TableData) => {
          assert.equal(err, null);
          assert.equal(result.length, 5000);
          done();
        }
      );
    });

    it("supports a partial path", function (done) {
      db.prepare('select * from "prepare.csv"').all((err: null | Error, result: TableData) => {
        assert.equal(err, null);
        assert.equal(result.length, 5000);
        done();
      });
    });
  });
});
