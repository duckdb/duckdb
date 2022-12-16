import * as sqlite3 from "../lib/duckdb";
import type { TableData } from "../lib/duckdb";
import * as assert from "assert";

const replacementScan = (table: string) => {
  if (table.endsWith(".csv")) {
    return null;
  } else {
    return {
      function: "read_csv_auto",
      parameter: `test/support/${table}.csv`,
    };
  }
};

const badReplacementScan1 = (table: string) => {
  return {
    function: "foo",
    parameter: "bar",
  };
};

const badReplacementScan2 = (table: string) => {
  return "hello" as unknown as sqlite3.ReplacementScanResult;
};

const badReplacementScan3 = (table: string) => {
  return {
    foo: "foo",
    bar: "bar",
  } as unknown as sqlite3.ReplacementScanResult;
};

describe("replacement scan", () => {
  var db: sqlite3.Database;
  describe("without replacement scan", () => {
    before(function (done) {
      db = new sqlite3.Database(":memory:", done);
    });

    it("is not found", (done) => {
      db.all(
        "SELECT * FROM 'prepare' LIMIT 5",
        function (err: null | Error, rows: TableData) {
          assert.notEqual(err, null);
          assert.match(err!.message, /Table with name prepare does not exist/);
          done();
        }
      );
    });
  });

  describe("with replacement scan", () => {
    before((done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.register_replacement_scan(replacementScan, done);
      });
    });

    it("is found when pattern matches", (done) => {
      db.all(
        "SELECT * FROM 'prepare' LIMIT 5",
        function (err: null | Error, rows: TableData) {
          assert.equal(rows.length, 5);
          done();
        }
      );
    });

    it("handles null response", (done) => {
      db.all(
        "SELECT * FROM 'test/support/prepare.csv' LIMIT 5",
        function (err: null | Error, rows: TableData) {
          assert.equal(rows.length, 5);
          done();
        }
      );
    });

    it("errors with invalid table", (done) => {
      db.all(
        "SELECT * FROM 'missing' LIMIT 5",
        function (err: null | Error, rows: TableData) {
          assert.notEqual(err, null);
          assert.match(
            err!.message,
            /No files found that match the pattern "test\/support\/missing.csv"/
          );
          done();
        }
      );
    });
  });

  describe("with invalid replacement scan functions", () => {
    it("does not crash with bad return values", (done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.register_replacement_scan(badReplacementScan1, () => {
          db.all(
            "SELECT * FROM 'missing' LIMIT 5",
            function (err: null | Error, rows: TableData) {
              assert.notEqual(err, null);
              assert.match(
                err!.message,
                /Table Function with name foo does not exist/
              );
              done();
            }
          );
        });
      });
    });

    it("does not crash with invalid response", (done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.register_replacement_scan(badReplacementScan2, () => {
          db.all(
            "SELECT * FROM 'missing' LIMIT 5",
            function (err: null | Error, rows: TableData) {
              assert.notEqual(err, null);
              assert.match(err!.message, /Invalid scan replacement result/);
              done();
            }
          );
        });
      });
    });

    it("does not crash with invalid response object", (done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.register_replacement_scan(badReplacementScan3, () => {
          db.all(
            "SELECT * FROM 'missing' LIMIT 5",
            function (err: null | Error, rows: TableData) {
              assert.notEqual(err, null);
              assert.match(
                err!.message,
                /Table Function with name undefined does not exist/
              );
              done();
            }
          );
        });
      });
    });
  });
});
