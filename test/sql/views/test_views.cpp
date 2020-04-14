#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test view creation", "[views]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (41), (42), (43)"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT i AS j FROM t1 WHERE i < 43"));
	REQUIRE_FAIL(con.Query("CREATE VIEW v1 AS SELECT 'whatever'"));

	result = con.Query("SELECT j FROM v1 WHERE j > 41");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(con.Query("DROP VIEW v1"));
	REQUIRE_FAIL(con.Query("SELECT j FROM v1 WHERE j > 41"));

	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT 'whatever'"));
	result = con.Query("SELECT * FROM v1");
	REQUIRE(CHECK_COLUMN(result, 0, {"whatever"}));

	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE VIEW v1 AS SELECT 42"));
	result = con.Query("SELECT * FROM v1");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	REQUIRE_NO_FAIL(con.Query("DROP VIEW v1"));
	REQUIRE_FAIL(con.Query("DROP VIEW v1"));
	REQUIRE_NO_FAIL(con.Query("DROP VIEW IF EXISTS v1"));

	REQUIRE_FAIL(con.Query("CREATE VIEW v1 AS SELECT * FROM dontexist"));
}

TEST_CASE("Test views with changing schema", "[views]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (41), (42), (43)"));
	// create a view that queries that table
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT * FROM t1"));

	result = con.Query("SELECT * FROM v1");
	REQUIRE(CHECK_COLUMN(result, 0, {41, 42, 43}));

	// now drop the table and create a table that has a different schema
	REQUIRE_NO_FAIL(con.Query("DROP TABLE t1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i DATE)"));

	// querying the view fails because the column types don't match the expected types
	REQUIRE_FAIL(con.Query("SELECT * FROM v1"));

	// now drop the table and create one that has extra columns
	REQUIRE_NO_FAIL(con.Query("DROP TABLE t1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER, j INTEGER)"));

	// again querying the view fails: there are extra columns present
	REQUIRE_FAIL(con.Query("SELECT * FROM v1"));

	// now drop the table and create one that has differently named columns
	REQUIRE_NO_FAIL(con.Query("DROP TABLE t1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(k INTEGER)"));

	// this is fine: types still match, and the original names will be applied as alias!
	result = con.Query("SELECT i FROM v1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE t1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));

	// now we can query again!
	result = con.Query("SELECT * FROM v1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}

TEST_CASE("Test deleting/updating views", "[views]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (41), (42), (43)"));
	// create a view
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT i AS j FROM t1 WHERE i < 43"));

	// try to delete from the view
	REQUIRE_FAIL(con.Query("DELETE FROM v1;"));
	// try to update the view
	REQUIRE_FAIL(con.Query("UPDATE v1 SET j=1;"));
}

TEST_CASE("Test view creation with alias", "[views]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (41), (42), (43)"));

	// this should fail because there are more aliases for the view than columns in the query
	REQUIRE_FAIL(con.Query("CREATE VIEW v1 (j, \"j2\") AS SELECT * FROM t1"));

	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 (j, \"j2\") AS SELECT i,i+1 FROM t1"));
	result = con.Query("SELECT j, j2 FROM v1");
	REQUIRE(CHECK_COLUMN(result, 0, {41, 42, 43}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 43, 44}));
	REQUIRE(result->types.size() == 2);
	REQUIRE_NO_FAIL(con.Query("DROP VIEW v1"));

	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 (j, \"j2\") AS SELECT i,i+1, i+2 FROM t1"));
	result = con.Query("SELECT j, j2 FROM v1");
	REQUIRE(result->types.size() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {41, 42, 43}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 43, 44}));
	REQUIRE_NO_FAIL(con.Query("DROP VIEW v1"));

	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 (j, \"j2\") AS SELECT i,i+1, i+2 as x FROM t1"));
	result = con.Query("SELECT j, j2, x FROM v1");
	REQUIRE(result->types.size() == 3);
	REQUIRE(CHECK_COLUMN(result, 0, {41, 42, 43}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 43, 44}));
	REQUIRE(CHECK_COLUMN(result, 2, {43, 44, 45}));

	REQUIRE_NO_FAIL(con.Query("DROP VIEW v1"));
}

TEST_CASE("Stacked views uh yeah", "[views]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (41), (42), (43), (44)"));

	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 (v1c1, v1c2) AS SELECT i,i+1 FROM t1 WHERE i > 41"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE VIEW v2 (v2c1, v2c2, v2c3) AS SELECT v1c1, v1c2, v1c1+v1c2 FROM v1 WHERE v1c2 > 42"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v3 (v3c1, v3c2) AS SELECT v2c1, v2c3 FROM v2 WHERE v2c1 > 43"));

	result = con.Query("SELECT v3c2+1 FROM v3 WHERE v3c1 > 42");

	REQUIRE(result->types.size() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {90}));
}
