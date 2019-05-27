#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Deletions", "[delete]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42);"));

	// delete everything
	REQUIRE_NO_FAIL(con.Query("DELETE FROM a;"));

	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

TEST_CASE("Test scan with large deletions", "[delete]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER);"));
	for(index_t i = 0; i < 10000; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (" + to_string(i) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// delete everything
	REQUIRE_NO_FAIL(con.Query("DELETE FROM a WHERE i >= 2000 AND i < 5000;"));

	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {7000}));
}
