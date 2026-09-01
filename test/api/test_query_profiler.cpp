#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/statement_iterator.hpp"

#include <iostream>
#include <thread>

using namespace duckdb;

TEST_CASE("Test query profiler", "[api]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	string output;

	con.EnableProfiling();
	// don't pollute the console with profiler info - write it to a file in the test directory instead.
	con.context->config.profiler_save_location = TestCreatePath("test_query_profiler_output.txt");

	string query = "SELECT * FROM (SELECT 42) tbl1, (SELECT 33) tbl2";
	REQUIRE_NO_FAIL(con.Query(query));

	output = con.GetProfilingInformation();
	REQUIRE(output.size() > 0);
	// the text profiler output renders only the operator tree (the query SQL is no longer included)

	output = con.GetProfilingInformation(ProfilerPrintFormat::JSON());
	REQUIRE(output.size() > 0);
	bool query_found_in_output = output.find(query) != std::string::npos;
	REQUIRE(query_found_in_output);
}

TEST_CASE("Test query profiler, no query in the profiling output.", "[api]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	string output;

	con.EnableProfiling();
	// don't pollute the console with profiler info - write it to a file in the test directory instead.
	con.context->config.profiler_save_location = TestCreatePath("test_query_profiler_output.txt");

	// Disable `QUERY_SQL` in profiling output by only tracking other metrics.
	REQUIRE_NO_FAIL(
	    con.Query("SET tracked_metrics = ['query.cpu_time', 'query.total_time', 'operator.timing', 'operator.type']"));
	string query = "SELECT * FROM (SELECT 42) tbl1, (SELECT 33) tbl2";
	REQUIRE_NO_FAIL(con.Query(query));

	output = con.GetProfilingInformation();
	REQUIRE(output.size() > 0);
	bool query_not_found_in_output = output.find(query) == std::string::npos;
	REQUIRE(query_not_found_in_output);

	output = con.GetProfilingInformation(ProfilerPrintFormat::JSON());
	REQUIRE(output.size() > 0);
	query_not_found_in_output = output.find(query) == std::string::npos;
	REQUIRE(query_not_found_in_output);
}

TEST_CASE("Test parser timing is reported per statement", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableProfiling();
	con.context->config.profiler_save_location = TestCreatePath("test_query_profiler_parser_output.txt");
	con.context->config.tracked_metrics = {"parser.total_time", "query.sql"};

	REQUIRE_NO_FAIL(con.Query("SELECT 7;"));
	auto single_statement_output = con.GetProfilingInformation(ProfilerPrintFormat::JSON());
	REQUIRE(single_statement_output.find("\"parser\"") != std::string::npos);
	REQUIRE(single_statement_output.find("SELECT 7;") != std::string::npos);

	auto iterator = con.context->IterateStatements("SELECT 42; SELECT 43;");
	for (const auto expected_query : {"SELECT 42; ", "SELECT 43;"}) {
		REQUIRE(iterator.Peek());
		auto statement = iterator.GetStatementForExecution();
		REQUIRE(statement);
		REQUIRE(statement->query == expected_query);
		REQUIRE_NO_FAIL(con.Query(std::move(statement)));

		auto output = con.GetProfilingInformation(ProfilerPrintFormat::JSON());
		REQUIRE(output.find("\"parser\"") != std::string::npos);
		REQUIRE(output.find(expected_query) != std::string::npos);
	}
	REQUIRE_FALSE(iterator.Peek());
}

TEST_CASE("Extracting statements does not start the query profiler", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableProfiling();
	con.context->config.profiler_save_location = TestCreatePath("test_query_profiler_extract_output.txt");
	con.context->config.tracked_metrics = {"parser.total_time", "query.sql"};

	auto statements = con.ExtractStatements("SELECT 44;");
	REQUIRE(statements.size() == 1);
	REQUIRE(QueryProfiler::Get(*con.context).GetQuerySQL().empty());

	REQUIRE_NO_FAIL(con.Query("SELECT 44;"));
	auto output = con.GetProfilingInformation(ProfilerPrintFormat::JSON());
	REQUIRE(output.find("\"parser\"") != std::string::npos);
	REQUIRE(output.find("SELECT 44;") != std::string::npos);
}

TEST_CASE("Test latency when interrupting query", "[api]") {
	// FIXME
	// duckdb::unique_ptr<QueryResult> result;
	// DuckDB db(nullptr);
	// Connection con(db);
	//
	// con.EnableProfiling();
	//
	// con.context->config.profiler_save_location = TestCreatePath("test_query_profiler_output.txt");
	//
	// // Test interupting a query and running a new one afterward.
	// // The latency should reflect the new one.
	// std::thread t([&con]() {
	// 	string query = "explain analyze select sum(range) from range(1_000_000_000);";
	// 	con.Query(query);
	// });
	//
	// std::this_thread::sleep_for(std::chrono::milliseconds(100));
	// con.Interrupt();
	// t.join();
	//
	// string query = "explain analyze select 42;";
	// REQUIRE_NO_FAIL(con.Query(query));
	//
	// auto profiling_info = con.GetProfilingTree()->GetProfilingInfo();
	// auto latency = profiling_info.GetMetricValue<double>(MetricType::LATENCY);
	// auto query_name = profiling_info.GetMetricValue<string>(MetricType::QUERY_NAME);
	// REQUIRE(query == query_name);
	// REQUIRE(latency > 0);
	// REQUIRE(latency < 0.1);
}
