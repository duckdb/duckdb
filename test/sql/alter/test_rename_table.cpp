#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test RENAME TABLE single transaction", "[alter]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (999), (100)"));

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

	auto result = con.Query("SELECT * FROM tbl2");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));
}

TEST_CASE("Test RENAME TABLE two parallel transactions", "[alter]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO tbl VALUES (999), (100)"));

	REQUIRE_NO_FAIL(con1.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE tbl RENAME TO tbl2"));

	REQUIRE_NO_FAIL(con1.Query("SELECT * FROM tbl2"));
	REQUIRE_FAIL(con1.Query("SELECT * FROM tbl"));

	REQUIRE_NO_FAIL(con2.Query("SELECT * FROM tbl"));
	REQUIRE_FAIL(con2.Query("SELECT * FROM tbl2"));

	REQUIRE_NO_FAIL(con1.Query("COMMIT"));
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	REQUIRE_FAIL(con1.Query("SELECT * FROM tbl"));
	REQUIRE_FAIL(con2.Query("SELECT * FROM tbl"));

	auto result = con1.Query("SELECT * FROM tbl2");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));

	result = con2.Query("SELECT * FROM tbl2");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));
}

TEST_CASE("Test RENAME TABLE four table rename and four parallel transactions", "[alter]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection c1(db);
	Connection c2(db);
	Connection c3(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl1 VALUES (999), (100)"));

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
	auto result = c1.Query("SELECT * FROM tbl1");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));

	// c2 sees ONLY tbl2
	REQUIRE_FAIL(c2.Query("SELECT * FROM tbl1"));
	REQUIRE_NO_FAIL(c2.Query("SELECT * FROM tbl2"));
	REQUIRE_FAIL(c2.Query("SELECT * FROM tbl3"));
	REQUIRE_FAIL(c2.Query("SELECT * FROM tbl4"));
	result = c2.Query("SELECT * FROM tbl2");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));

	// c3 sees ONLY tbl3
	REQUIRE_FAIL(c3.Query("SELECT * FROM tbl1"));
	REQUIRE_FAIL(c3.Query("SELECT * FROM tbl2"));
	REQUIRE_NO_FAIL(c3.Query("SELECT * FROM tbl3"));
	REQUIRE_FAIL(c3.Query("SELECT * FROM tbl4"));
	result = c3.Query("SELECT * FROM tbl3");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));

	// con sees ONLY tbl4
	REQUIRE_FAIL(con.Query("SELECT * FROM tbl1"));
	REQUIRE_FAIL(con.Query("SELECT * FROM tbl2"));
	REQUIRE_FAIL(con.Query("SELECT * FROM tbl3"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM tbl4"));
	result = con.Query("SELECT * FROM tbl4");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));
}

TEST_CASE("Test RENAME TABLE with a view as entry", "[alter]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (999), (100)"));

	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT * FROM tbl"));
	REQUIRE_FAIL(con.Query("ALTER TABLE v1 RENAME TO v2"));
	auto result = con.Query("SELECT * FROM  v1");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 100}));
}

TEST_CASE("Test RENAME TABLE: table does not exist and rename to an already existing table", "[alter]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl2(i INTEGER)"));
	// Renaming a non existing table
	REQUIRE_FAIL(con.Query("ALTER TABLE non_table RENAME TO tbl"));
	// rename to an already existing table
	REQUIRE_FAIL(con.Query("ALTER TABLE tbl2 RENAME TO tbl"));
}

TEST_CASE("Test RENAME TABLE with constraints", "[alter]") {
	DuckDB db(nullptr);
	Connection con(db);
	// create a table with a check constraint
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER PRIMARY KEY, j INTEGER CHECK(j < 10))"));

	// check primary key constrain
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (999, 4), (1000, 5)"));
	REQUIRE_FAIL(con.Query("INSERT INTO tbl VALUES (999, 4), (1000, 5)"));

	// check value constrain (j < 10)
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (9999, 0), (10000, 1)"));
	REQUIRE_FAIL(con.Query("INSERT INTO tbl VALUES (777, 10), (888, 10)"));

	auto result = con.Query("SELECT * FROM tbl");
	REQUIRE(CHECK_COLUMN(result, 0, {999, 1000, 9999, 10000}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 0, 1}));

	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl RENAME TO new_tbl"));
	// insert two conflicting pairs at the same time
	REQUIRE_FAIL(con.Query("INSERT INTO new_tbl VALUES (999, 0), (1000, 1)"));
	// insert two conflicting pairs at the same time
	REQUIRE_FAIL(con.Query("INSERT INTO new_tbl VALUES (9999, 0), (10000, 1)"));

	// insert values out of range constrain
	REQUIRE_FAIL(con.Query("INSERT INTO new_tbl VALUES (1, 10), (2, 999)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO new_tbl VALUES (66, 6), (55, 5)"));
}
