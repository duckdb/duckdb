#include "catch.hpp"
#include "test_helpers.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test query profiler", "[api]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	string output;

	con.EnableQueryVerification();
	con.EnableProfiling();
	// don't pollute the console with profiler info.
	con.context->config.emit_profiler_output = false;

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM (SELECT 42) tbl1, (SELECT 33) tbl2"));

	output = con.GetProfilingInformation();
	REQUIRE(output.size() > 0);

	output = con.GetProfilingInformation(ProfilerPrintFormat::JSON);
	REQUIRE(output.size() > 0);
}
