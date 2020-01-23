#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test failure cases in table creation/deletion", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// primary key constraint that references unknown column
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(j))"));
	// primary key that references the same key twice
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(i, i))"));
	// multiple primary keys
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(i), PRIMARY KEY(i)"));
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, PRIMARY KEY(i)"));
}

TEST_CASE("Test temporary table creation", "[catalog]") {

	unique_ptr<QueryResult> result;

	// see if temp tables survive restart
	FileSystem fs;
	string db_folder = TestCreatePath("temptbls");

	{
		DuckDB db_p(db_folder);
		Connection con_p(db_p);
		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY TABLE temp.a (i INTEGER)"));
		REQUIRE_NO_FAIL(con_p.Query("INSERT INTO a VALUES (42)"));
		REQUIRE_NO_FAIL(con_p.Query("DELETE FROM a"));
		REQUIRE_NO_FAIL(con_p.Query("DELETE FROM temp.a"));
		REQUIRE_FAIL(con_p.Query("DELETE FROM asdf.a"));

		REQUIRE_NO_FAIL(con_p.Query("INSERT INTO temp.a VALUES (43)"));

		REQUIRE_NO_FAIL(con_p.Query("UPDATE temp.a SET i = 44"));
		REQUIRE_NO_FAIL(con_p.Query("UPDATE a SET i = 45"));

		// TODO also update here, but no code for this yet in this branch

		result = con_p.Query("SELECT COUNT(*) from a");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}

	{
		DuckDB db_p(db_folder);
		Connection con_p(db_p);
		REQUIRE_FAIL(con_p.Query("SELECT * FROM a"));
		result = con_p.Query("CREATE TEMPORARY TABLE a (i INTEGER)");
		REQUIRE_NO_FAIL(con_p.Query("SELECT * FROM a"));
		result = con_p.Query("SELECT COUNT(*) from a");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}

	DuckDB db(nullptr);
	Connection con(db);

	// basic temp table creation works
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER) ON COMMIT PRESERVE ROWS"));
	// we can (but never are required to) prefix temp tables with "temp" schema
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integersx(i INTEGER)"));
	// we can't prefix temp tables with a schema that is not "temp"
	REQUIRE_FAIL(con.Query("CREATE TEMPORARY TABLE asdf.integersy(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE s1 AS SELECT 42"));

	REQUIRE_FAIL(con.Query("CREATE TABLE temp.integersy(i INTEGER)"));

	REQUIRE_FAIL(con.Query("CREATE SCHEMA temp"));

	REQUIRE_FAIL(con.Query("DROP TABLE main.integersx"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integersx"));

	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE temp.integersx(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE temp.integersx"));

	// unsupported because stupid
	REQUIRE_FAIL(con.Query("CREATE TEMPORARY TABLE integers2(i INTEGER) ON COMMIT DELETE ROWS"));

	// temp table already exists
	REQUIRE_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (42)"));
	result = con.Query("SELECT i from integers");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// temp table survives commit
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers2(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers2 VALUES (42)"));
	result = con.Query("SELECT i from integers2");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT i from integers2");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// temp table does not survive rollback
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers3(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers3 VALUES (42)"));
	result = con.Query("SELECT i from integers3");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	REQUIRE_FAIL(con.Query("SELECT i from integers3"));

	Connection con2(db);
	// table is not visible to other cons
	REQUIRE_FAIL(con2.Query("INSERT INTO integers VALUES (42)"));
}

// TODO sequences?
