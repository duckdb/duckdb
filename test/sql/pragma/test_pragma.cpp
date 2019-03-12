#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test PRAGMA parsing", "[pragma]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

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

TEST_CASE("Test PRAGMA enable_profiling parsing", "[pragma]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable and disable profiling
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_profiling"));
	// enable profiling cannot be called
	REQUIRE_FAIL(con.Query("PRAGMA enable_profiling()"));
	// but we can assign the format of the output
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling=json"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling=query_tree"));
	REQUIRE_FAIL(con.Query("PRAGMA enable_profiling=unsupported"));
	// select the location of where to save the profiling output (instead of printing to stdout)
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_output=test.json"));
	// cannot be set like this
	REQUIRE_FAIL(con.Query("PRAGMA profiling_output"));
	// but we can clear it again
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_output="));
}
