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
    const values = [true, false];
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
    db.prepare(`SELECT 
    INTERVAL 1 MINUTE as minutes,
    INTERVAL 5 DAY as days,
    INTERVAL 4 MONTH as months,
    INTERVAL 4 MONTH + INTERVAL 5 DAY + INTERVAL 1 MINUTE as combined;`).each((err, row) => {
      assert(err === null);
      assert.deepEqual(row.minutes, { months: 0, days: 0, micros: 60 * 1000 * 1000});
      assert.deepEqual(row.days, { months: 0, days: 5, micros: 0});
      assert.deepEqual(row.months, {months: 4, days: 0, micros: 0});
      assert.deepEqual(row.combined, {months: 4, days: 5, micros: 60 * 1000 * 1000});
      done();
    });
  });
});
