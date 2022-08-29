var sqlite3 = require("..");
var assert = require("assert");

describe("data type support", function () {
  let db;
  before(function (done) {
    db = new sqlite3.Database(":memory:", done);
  });

  it("supports BOOLEAN values", function (done) {
    db.run("CREATE TABLE boolean_table (i BOOLEAN)");
    const stmt = db.prepare("INSERT INTO boolean_table VALUES (?)");
    const values = [true, false, null];
    values.forEach((bool) => {
      stmt.run(bool);
    });
    db.prepare("SELECT i from boolean_table;").all((err, res) => {
      assert(err === null);
      assert(res.every((v, i) => v.i === values[i]));
      done();
    });
  });
  it("supports INTERVAL values", function (done) {
    db.prepare(
      `SELECT
    INTERVAL 1 MINUTE as minutes,
    INTERVAL 5 DAY as days,
    INTERVAL 4 MONTH as months,
    INTERVAL 4 MONTH + INTERVAL 5 DAY + INTERVAL 1 MINUTE as combined;`
    ).each((err, row) => {
      assert(err === null);
      assert.deepEqual(row.minutes, {
        months: 0,
        days: 0,
        micros: 60 * 1000 * 1000,
      });
      assert.deepEqual(row.days, { months: 0, days: 5, micros: 0 });
      assert.deepEqual(row.months, { months: 4, days: 0, micros: 0 });
      assert.deepEqual(row.combined, {
        months: 4,
        days: 5,
        micros: 60 * 1000 * 1000,
      });
      done();
    });
  });

  it("supports STRUCT values", function (done) {
    db.prepare(`SELECT {'x': 1, 'y': 2, 'z': {'a': 'b'}} as struct`).each(
      (err, row) => {
        assert.deepEqual(row.struct, { x: 1, y: 2, z: { a: "b" } });
        done();
      }
    );
  });

  it("supports STRUCT values with NULL", function (done) {
    db.run("CREATE TABLE struct_table (s STRUCT(a VARCHAR, b BOOLEAN))");
    db.run("INSERT INTO struct_table VALUES ({'a': 'hello', 'b': true})");
    db.run("INSERT INTO struct_table VALUES ({'a': 'goodbye', 'b': false})");
    db.run("INSERT INTO struct_table VALUES ({'a': 'aloha', 'b': NULL})");
    db.prepare("SELECT s from struct_table;").all((err, res) => {
      assert(err === null);
      assert.deepEqual(res, [
        { s: { a: "hello", b: true } },
        { s: { a: "goodbye", b: false } },
        { s: { a: "aloha", b: null } },
      ]);
      done();
    });
  });

  it("recursively supports NULL values", function (done) {
    db.run(
      `CREATE TABLE recursive_struct AS SELECT [
      { 'a': 42, 'b': [1, 2, 3]},
      NULL,
      { 'a': NULL, 'b': [4, NULL, 6]},
      {'a': 43, 'b': NULL}
   ] l UNION ALL SELECT NULL`
    );
    db.prepare("SELECT l from recursive_struct").all((err, res) => {
      assert(err === null);
      assert.deepEqual(res, [
        {
          l: [
            {
              a: 42,
              b: [1, 2, 3],
            },
            null,
            {
              a: null,
              b: [4, null, 6],
            },
            {
              a: 43,
              b: null,
            },
          ],
        },
        {
          l: null,
        },
      ]);
      done();
    });
  });

  it("supports LIST values", function (done) {
    db.prepare(`SELECT ['duck', 'duck', 'goose'] as list`).each((err, row) => {
      assert(err === null);
      assert.deepEqual(row.list, ["duck", "duck", "goose"]);
      done();
    });
  });

  it("supports LIST with NULL values", function (done) {
    db.prepare(`SELECT ['duck', 'duck', NULL] as list`).each((err, row) => {
      assert(err === null);
      assert.deepEqual(row.list, ["duck", "duck", null]);
      done();
    });
  });

  it("supports DATE values", function (done) {
    db.prepare(`SELECT '2021-01-01'::DATE as dt;`).each((err, row) => {
      assert(err === null);
      assert.deepEqual(row.dt, new Date(Date.UTC(2021, 0, 1)));
      done();
    });
  });
  it("supports TIMESTAMP values", function (done) {
    db.prepare(`SELECT '2021-01-01T00:00:00'::TIMESTAMP as ts;`).each(
      (err, row) => {
        assert(err === null);
        assert.deepEqual(row.ts, new Date(Date.UTC(2021, 0, 1)));
        done();
      }
    );
  });
  it("supports TIMESTAMP WITH TIME ZONE values", function (done) {
    db.prepare(`SELECT '2021-01-01T00:00:00Z'::TIMESTAMPTZ as tstz;`).each(
      (err, row) => {
        assert(err === null);
        assert.deepEqual(row.tstz, new Date(Date.UTC(2021, 0, 1)));
        done();
      }
    );
  });
  it("supports DECIMAL values", function (done) {
    db.run("CREATE TABLE decimal_table (d DECIMAL(24, 6))");
    const stmt = db.prepare("INSERT INTO decimal_table VALUES (?)");
    const values = [0, -1, 23534642362547.543463, null];
    values.forEach((d) => {
      stmt.run(d);
    });
    db.prepare("SELECT d from decimal_table;").all((err, res) => {
      assert(err === null);
      assert(res.every((v, i) => v.d === values[i]));
      done();
    });
  });
});
