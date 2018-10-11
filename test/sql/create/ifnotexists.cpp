
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test IF NOT EXISTS", "[create]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// CREATE TABLE IF NOT EXISTS
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE IF NOT EXISTS integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE IF NOT EXISTS integers(i INTEGER, j INTEGER)"));

	// DROP TABLE IF EXISTS
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS integers"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS integers"));
}
