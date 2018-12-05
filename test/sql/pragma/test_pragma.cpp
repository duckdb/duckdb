#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test PRAGMA parsing", "[pragma]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// Almost pragma but not quite
	REQUIRE_FAIL(con.Query("PRAG"));
	// Pragma without a keyword
	REQUIRE_FAIL(con.Query("PRAGMA "));
	// Unknown pragma error
	REQUIRE_FAIL(con.Query("PRAGMA random_unknown_pragma"));
	// Call pragma in wrong way
	REQUIRE_FAIL(con.Query("PRAGMA table_info = 3"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// Now it should work
	REQUIRE_NO_FAIL(con.Query("PRAGMA table_info('integers');"));
	// Parsing also works with extra spaces
	REQUIRE_NO_FAIL(con.Query("  PRAGMA    table_info  ('integers');"));
}
