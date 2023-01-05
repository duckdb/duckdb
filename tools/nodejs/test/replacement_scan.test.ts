import * as sqlite3 from "../lib/duckdb";
import type { TableData } from "../lib/duckdb";
import { expect } from "chai";

const replacementScan = (table: string) => {
  if (table.endsWith(".csv")) {
    return null;
  } else {
    return {
      function: "read_csv_auto",
      parameters: [`test/support/${table}.csv`],
    };
  }
};

const invalidTableFunction = (table: string) => {
  return {
    function: "foo",
    parameters: ["bar"],
  };
};

const invalidResultType = (table: string) => {
  return "hello" as unknown as sqlite3.ReplacementScanResult;
};

const invalidResultKeys = (table: string) => {
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
          expect(err).not.to.be.null;
          expect(err!.message).to.match(
            /Table with name prepare does not exist/
          );
          done();
        }
      );
    });
  });

  describe("with replacement scan", () => {
    before((done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.registerReplacementScan(replacementScan).then(done);
      });
    });

    it("is found when pattern matches", (done) => {
      db.all(
        "SELECT * FROM 'prepare' LIMIT 5",
        function (err: null | Error, rows: TableData) {
          expect(rows.length).to.equal(5);
          done();
        }
      );
    });

    it("handles null response", (done) => {
      db.all(
        "SELECT * FROM 'test/support/prepare.csv' LIMIT 5",
        function (err: null | Error, rows: TableData) {
          expect(rows.length).to.equal(5);
          done();
        }
      );
    });

    it("errors with invalid table", (done) => {
      db.all(
        "SELECT * FROM 'missing' LIMIT 5",
        function (err: null | Error, rows: TableData) {
          expect(err).not.to.be.null;
          expect(err!.message).to.match(
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
        db.registerReplacementScan(invalidTableFunction).then(() => {
          db.all(
            "SELECT * FROM 'missing' LIMIT 5",
            function (err: null | Error, rows: TableData) {
              expect(err).not.to.be.null;
              expect(err!.message).to.match(
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
        db.registerReplacementScan(invalidResultType).then(() => {
          db.all(
            "SELECT * FROM 'missing' LIMIT 5",
            function (err: null | Error, rows: TableData) {
              expect(err).not.to.be.null;
              expect(err!.message).to.match(/Invalid scan replacement result/);
              done();
            }
          );
        });
      });
    });

    it("does not crash with invalid response object", (done) => {
      db = new sqlite3.Database(":memory:", () => {
        db.registerReplacementScan(invalidResultKeys).then(() => {
          db.all(
            "SELECT * FROM 'missing' LIMIT 5",
            function (err: null | Error, rows: TableData) {
              expect(err).not.to.be.null;
              expect(err!.message).to.match(/Expected parameter array/);
              done();
            }
          );
        });
      });
    });
  });
});
