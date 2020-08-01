#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("PREPARE and WAL", "[prepared][.]") {
	unique_ptr<QueryResult> result;
	auto prepare_database = TestCreatePath("prepare_test");

	// make sure the database does not exist
	DeleteDatabase(prepare_database);
	{
		// create a database and insert values
		DuckDB db(prepare_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (a INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("PREPARE p1 AS INSERT INTO t VALUES ($1)"));
		REQUIRE_NO_FAIL(con.Query("EXECUTE p1(42)"));
		REQUIRE_NO_FAIL(con.Query("EXECUTE p1(43)"));
		REQUIRE_NO_FAIL(con.Query("DEALLOCATE p1"));
		result = con.Query("SELECT a FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	}
	{
		DuckDB db(prepare_database);
		Connection con(db);

		result = con.Query("SELECT a FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));

		// unhelpfully use the same statement name again, it should be available, but do nothing with it
		REQUIRE_NO_FAIL(con.Query("PREPARE p1 AS DELETE FROM t WHERE a=$1"));
	}
	// reload the database from disk
	{
		DuckDB db(prepare_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("PREPARE p1 AS DELETE FROM t WHERE a=$1"));
		REQUIRE_NO_FAIL(con.Query("EXECUTE p1(43)"));

		result = con.Query("SELECT a FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	// reload again

	{
		DuckDB db(prepare_database);
		Connection con(db);

		result = con.Query("SELECT a FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}

	{
		DuckDB db(prepare_database);
		Connection con(db);

		result = con.Query("SELECT a FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));

		REQUIRE_NO_FAIL(con.Query("PREPARE p1 AS UPDATE t SET a = $1"));
		REQUIRE_NO_FAIL(con.Query("EXECUTE p1(43)"));

		result = con.Query("SELECT a FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {43}));
	}
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(prepare_database);
		Connection con(db);

		result = con.Query("SELECT a FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {43}));
	}
	DeleteDatabase(prepare_database);
}
