#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test RENAME TABLE single transaction", "[alter]") {
    DuckDB db(nullptr);
    Connection con(db);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
    REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
    REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl RENAME TO tbl2"));
    REQUIRE_NO_FAIL(con.Query("SELECT * FROM tbl2"));
    REQUIRE_FAIL(con.Query("SELECT * FROM tbl"));
    REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
    REQUIRE_NO_FAIL(con.Query("SELECT * FROM tbl;"));
    REQUIRE_FAIL(con.Query("SELECT * FROM tbl2"));
    REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
    REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl RENAME TO tbl2"));
    REQUIRE_NO_FAIL(con.Query("COMMIT"));
    REQUIRE_NO_FAIL(con.Query("SELECT * FROM tbl2"));
    REQUIRE_FAIL(con.Query("SELECT * FROM tbl"));
}

TEST_CASE("Test RENAME TABLE two parallel transactions", "[alter]") {
    DuckDB db(nullptr);
    Connection con1(db);
    Connection con2(db);
    REQUIRE_NO_FAIL(con1.Query("CREATE TABLE tbl(i INTEGER)"));
    REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (3)"));

    REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
    REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
    REQUIRE_NO_FAIL(con1.Query("ALTER TABLE tbl RENAME TO tbl2"));

    REQUIRE_NO_FAIL(con1.Query("SELECT * FROM tbl2"));
    REQUIRE_FAIL(con1.Query("SELECT * FROM tbl"));

    REQUIRE_NO_FAIL(con2.Query("SELECT * FROM tbl"));
    REQUIRE_FAIL(con2.Query("SELECT * FROM tbl2"));

    REQUIRE_NO_FAIL(con1.Query("COMMIT"));
    REQUIRE_NO_FAIL(con2.Query("COMMIT"));

    REQUIRE_NO_FAIL(con1.Query("SELECT * FROM tbl2"));
    REQUIRE_NO_FAIL(con2.Query("SELECT * FROM tbl2"));
}

TEST_CASE("Test RENAME TABLE four table rename and four parallel transactions", "[alter]") {
    DuckDB db(nullptr);
    Connection con(db);
    Connection c1(db);
    Connection c2(db);
    Connection c3(db);

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl1(i INTEGER)"));

    // rename chain
    // c1 starts a transaction now
    REQUIRE_NO_FAIL(c1.Query("BEGIN TRANSACTION"));
    // rename in con, c1 should still see "tbl1"
    REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl1 RENAME TO tbl2"));

    // c2 starts a transaction now
    REQUIRE_NO_FAIL(c2.Query("BEGIN TRANSACTION"));
    // rename in con, c2 should still see "tbl2"
    REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl2 RENAME TO tbl3"));

    // c3 starts a transaction now
    REQUIRE_NO_FAIL(c3.Query("BEGIN TRANSACTION"));
    // rename in con, c3 should still see "tbl3"
    REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl3 RENAME TO tbl4"));

    // c1 sees ONLY tbl1
    REQUIRE_NO_FAIL(c1.Query("SELECT * FROM tbl1"));
    REQUIRE_FAIL(c1.Query("SELECT * FROM tbl2"));
    REQUIRE_FAIL(c1.Query("SELECT * FROM tbl3"));
    REQUIRE_FAIL(c1.Query("SELECT * FROM tbl4"));

    // c2 sees ONLY tbl2
    REQUIRE_FAIL(c2.Query("SELECT * FROM tbl1"));
    REQUIRE_NO_FAIL(c2.Query("SELECT * FROM tbl2"));
    REQUIRE_FAIL(c2.Query("SELECT * FROM tbl3"));
    REQUIRE_FAIL(c2.Query("SELECT * FROM tbl4"));

    // c3 sees ONLY tbl3
    REQUIRE_FAIL(c3.Query("SELECT * FROM tbl1"));
    REQUIRE_FAIL(c3.Query("SELECT * FROM tbl2"));
    REQUIRE_NO_FAIL(c3.Query("SELECT * FROM tbl3"));
    REQUIRE_FAIL(c3.Query("SELECT * FROM tbl4"));

    // con sees ONLY tbl4
    REQUIRE_FAIL(con.Query("SELECT * FROM tbl1"));
    REQUIRE_FAIL(con.Query("SELECT * FROM tbl2"));
    REQUIRE_FAIL(con.Query("SELECT * FROM tbl3"));
    REQUIRE_NO_FAIL(con.Query("SELECT * FROM tbl4"));
}

TEST_CASE("Test RENAME TABLE with a view as entry", "[alter]") {
    DuckDB db(nullptr);
    Connection con(db);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
    REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (3)"));

    REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT * FROM tbl"));
    REQUIRE_FAIL(con.Query("ALTER TABLE v1 RENAME TO v2"));
    REQUIRE_NO_FAIL(con.Query("SELECT * FROM v1"));
}