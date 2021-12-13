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
});
