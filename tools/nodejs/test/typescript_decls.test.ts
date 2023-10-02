import * as duckdb from "..";
import assert from "assert";
import fs from "fs";

describe("TypeScript declarations", function () {
  var db: duckdb.Database;
  before(function (done) {
    db = new duckdb.Database(":memory:", duckdb.OPEN_READWRITE, done);
  });

  it("typescript: Database constructor no callback", (done) => {
    const tdb0 = new duckdb.Database(":memory:"); // no callback argument
    done();
  });

  it("Database.create -- read only flag", (done) => {
    const roDb = new duckdb.Database(
      ":memory:",
      duckdb.OPEN_READONLY,
      (err: duckdb.DuckDbError | null, res: any) => {
        assert.equal(err?.code, "DUCKDB_NODEJS_ERROR");
        assert.equal(err?.errno, -1);
        const errMessage: string = err?.message ?? "";
        assert(
          errMessage.includes(
            "Cannot launch in-memory database in read-only mode"
          )
        );
        done();
      }
    );
  });

  it("typescript: Database constructor path error", (done) => {
    const tdb0 = new duckdb.Database(
      "./bogusPath.db",
      duckdb.OPEN_READWRITE,
      (err, res) => {
        // Issue: I'm a little surprised that specifying an invalid file path
        // doesn't seem to immediately signal an error here, but it doesn't.
        tdb0.all(
          "PRAGMA show_tables",
          (err: duckdb.DuckDbError | null, res: any) => {
            done();
          }
        );
      }
    );
  });

  it("typescript: query with error", (done) => {
    // query with an error:
    db.all(
      "SELECT * FROM sometable",
      (err: duckdb.DuckDbError | null, res: any) => {
        assert.equal(err?.code, "DUCKDB_NODEJS_ERROR");
        assert.equal(err?.errno, -1);
        done();
      }
    );
  });

  it("typescript: Database#exec", function (done) {
    var sql = fs.readFileSync("test/support/script.sql", "utf8");
    db.exec(sql, function (err: duckdb.DuckDbError | null) {
      if (err) throw err;
      done();
    });
  });

  it("typescript: retrieve database structure", function (done) {
    db.all(
      "SELECT type, name FROM sqlite_master ORDER BY type, name",
      function (err: duckdb.DuckDbError | null, rows: duckdb.TableData) {
        if (err) throw err;
        assert.deepEqual(rows, [
          { type: "table", name: "grid_key" },
          { type: "table", name: "grid_utfgrid" },
          { type: "table", name: "images" },
          { type: "table", name: "keymap" },
          { type: "table", name: "map" },
          { type: "table", name: "metadata" },
          { type: "view", name: "grid_data" },
          { type: "view", name: "grids" },
          { type: "view", name: "tiles" },
        ]);
        done();
      }
    );
  });

  it("typescript: database#all with no callback", (done) => {
    db.all("select 42 as x");
    done();
  });

  it("typescript: database#connect", (done) => {
    const conn = db.connect();
    assert(conn instanceof duckdb.Connection);
    done();
  });

  it("typescript: ensure empty results work ok", (done) => {
    db.all(
      "create table test_table as select 42 as x",
      (err: duckdb.DuckDbError | null, res: duckdb.TableData) => {
        db.all(
          "drop table test_table",
          (err: duckdb.DuckDbError | null, res: duckdb.TableData) => {
            console.log("drop table results: ", err, res);
            assert.deepEqual(res, []);
            done();
          }
        );
      }
    );
  });

  it("typescript: ternary int udf", function (done) {
    db.register_udf(
      "udf",
      "integer",
      (x: number, y: number, z: number) => x + y + z
    );
    db.all(
      "select udf(21, 20, 1) v",
      function (err: duckdb.DuckDbError | null, rows: duckdb.TableData) {
        if (err) throw err;
        assert.equal(rows[0].v, 42);
      }
    );
    db.unregister_udf("udf", done);
  });
  it("typescript: retrieve 100,000 rows with Statement#each", function (done) {
    var total = 100000;
    var retrieved = 0;

    db.each(
      "SELECT * FROM range(0, ?)",
      total,
      function (err: duckdb.DuckDbError | null, row: any) {
        if (err) throw err;
        retrieved++;

        if (retrieved === total) {
          assert.equal(
            retrieved,
            total,
            "Only retrieved " + retrieved + " out of " + total + " rows."
          );
          done();
        }
      }
    );
  });
});

describe("typescript: simple prepared statement", function () {
  var db: duckdb.Database;
  before(function (done) {
    db = new duckdb.Database(":memory:", duckdb.OPEN_READWRITE, done);
  });

  it("should prepare, run and finalize the statement", function (done) {
    db.prepare("CREATE TABLE foo (bar text)").run().finalize(done);
  });

  after(function (done) {
    db.close(done);
  });
});

describe("typescript: prepared statements", function () {
  var db: duckdb.Database;
  before(function (done) {
    db = new duckdb.Database(":memory:", duckdb.OPEN_READWRITE, done);
  });

  var inserted = 0;
  var retrieved = 0;

  // We insert and retrieve that many rows.
  var count = 1000;

  it("typescript: should create the table", function (done) {
    db.prepare("CREATE TABLE foo (txt text, num int, flt double, blb blob)")
      .run()
      .finalize(done);
  });

  it("typescript: should insert " + count + " rows", function (done) {
    for (var i = 0; i < count; i++) {
      db.prepare("INSERT INTO foo VALUES(?, ?, ?, ?)")
        .run(
          "String " + i,
          i,
          i * Math.PI,
          null,
          function (err: duckdb.DuckDbError | null) {
            if (err) throw err;
            inserted++;
          }
        )
        .finalize(function (err) {
          if (err) throw err;
          if (inserted == count) done();
        });
    }
  });
});

describe("typescript: stream and QueryResult", function () {
  const total = 1000;

  let db: duckdb.Database;
  let conn: duckdb.Connection;
  before((done) => {
    db = new duckdb.Database(":memory:", duckdb.OPEN_READWRITE, () => {
      conn = new duckdb.Connection(db, done);
    });
  });

  it("streams results", async () => {
    let retrieved = 0;
    const stream = conn.stream("SELECT * FROM range(0, ?)", total);
    for await (const row of stream) {
      retrieved++;
    }
    assert.equal(total, retrieved);
  });
});
