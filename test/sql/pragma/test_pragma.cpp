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

	// enable profiling cannot be called
	REQUIRE_FAIL(con.Query("PRAGMA enable_profiling()"));
	REQUIRE_FAIL(con.Query("PRAGMA enable_profiling='unsupported'"));
	REQUIRE_FAIL(con.Query("PRAGMA profiling_output"));
	// select the location of where to save the profiling output (instead of printing to stdout)
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_output='test.json'"));
	// but we can clear it again
	REQUIRE_NO_FAIL(con.Query("PRAGMA profiling_output=''"));
	// enable and disable profiling
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_profiling"));
	// REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling"));
}

TEST_CASE("Test PRAGMA memory_limit", "[pragma]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// set memory_limit
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1GB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit=-1"));
	// different units can be used
	// G and GB = gigabyte
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1G'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1GB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1gb'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit = '1GB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1.0gb'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1.0 gb'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1.0 gigabytes'"));
	// M and MB = megabyte
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='100M'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='100MB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='100mb'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='100 megabytes'"));
	// K and KB = kilobyte
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='10000K'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='10000KB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='10000kb'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='10000 kilobytes'"));
	// B = byte
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='100000B'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='100000b'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='100000 bytes'"));
	// T and TB = terabyte
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='0.01T'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='0.01TB'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='0.01tb'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='0.01 terabytes'"));
	// no unit or unknown units fail
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit=100"));
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='0.01BG'"));
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='0.01BLA'"));
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='0.01PP'"));
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='0.01TEST'"));
	// we can't invoke it like this either
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit"));
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit()"));
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit(1, 2)"));
}
