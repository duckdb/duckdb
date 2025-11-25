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

	string query = "SELECT * FROM (SELECT 42) tbl1, (SELECT 33) tbl2";
	REQUIRE_NO_FAIL(con.Query(query));

	output = con.GetProfilingInformation();
	REQUIRE(output.size() > 0);
	bool query_found_in_output = output.find(query) != std::string::npos;
	REQUIRE(query_found_in_output);

	output = con.GetProfilingInformation(ProfilerPrintFormat::JSON);
	REQUIRE(output.size() > 0);
	query_found_in_output = output.find(query) != std::string::npos;
	REQUIRE(query_found_in_output);
}

TEST_CASE("Test query profiler, no query in the profiling output.", "[api]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	string output;

	con.EnableQueryVerification();
	con.EnableProfiling();
	// don't pollute the console with profiler info.
	con.context->config.emit_profiler_output = false;

	// Disable `QUERY_NAME` in profiling output.
	REQUIRE_NO_FAIL(con.Query(R"(PRAGMA custom_profiling_settings = '{"QUERY_NAME": "false"}')"));
	string query = "SELECT * FROM (SELECT 42) tbl1, (SELECT 33) tbl2";
	REQUIRE_NO_FAIL(con.Query(query));

	output = con.GetProfilingInformation();
	REQUIRE(output.size() > 0);
	bool query_not_found_in_output = output.find(query) == std::string::npos;
	REQUIRE(query_not_found_in_output);

	output = con.GetProfilingInformation(ProfilerPrintFormat::JSON);
	REQUIRE(output.size() > 0);
	query_not_found_in_output = output.find(query) == std::string::npos;
	REQUIRE(query_not_found_in_output);
}
