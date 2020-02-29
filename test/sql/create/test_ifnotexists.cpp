#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test IF NOT EXISTS", "[create]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// CREATE TABLE IF NOT EXISTS
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE IF NOT EXISTS integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE IF NOT EXISTS integers(i INTEGER, j INTEGER)"));

	// IF NOT EXISTS with CREATE TABLE AS
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE IF NOT EXISTS integers2 AS SELECT 42"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE IF NOT EXISTS integers2 AS SELECT 42"));

	// DROP TABLE IF EXISTS
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS integers"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS integers"));
}
