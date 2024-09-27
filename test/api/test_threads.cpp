#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/virtual_file_system.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

void run_query_multiple_times(duckdb::unique_ptr<string> query, duckdb::unique_ptr<Connection> con) {
	for (int i = 0; i < 10; ++i) {
		auto result = con->Query(*query);
	}
}

void change_thread_counts(duckdb::DuckDB &db) {
	auto con = Connection(db);
	for (int i = 0; i < 10; ++i) {
		con.Query("SET threads=10");
		con.Query("SET threads=1");
	}
}

// NumberOfThreads acquired the same lock as RelaunchThreads
// NumberOfThreads is waiting for the lock
// RelaunchThreads is waiting on the thread to finish, while holding the lock
TEST_CASE("Test deadlock issue between NumberOfThreads and RelaunchThreads", "[api]") {
	duckdb::DuckDB db(nullptr);

	int thread_count = 10;
	std::vector<std::thread> threads(thread_count);

	// This query will hit NumberOfThreads because it uses the RadixPartitionedHashtable
	for (int i = 0; i < thread_count; ++i) {
		auto query = make_uniq<string>(R"(
			WITH dataset AS (
			  SELECT * FROM (VALUES
				(1, 'Alice'),
				(2, 'Bob'),
				(3, 'Alice'),
				(4, 'Carol')
			  ) AS t(id, name)
			)
			SELECT DISTINCT name FROM dataset;
		)");

		threads[i] = std::thread(run_query_multiple_times, std::move(query), make_uniq<Connection>(db));
	}

	// Fire off queries that change the thread count,
	// causing us to relaunch the worker threads on every subsequent query.
	change_thread_counts(db);

	for (int i = 0; i < thread_count; ++i) {
		threads[i].join();
	}
}

TEST_CASE("Test database maximum_threads argument", "[api]") {
	// default is number of hw threads
	// FIXME: not yet
	{
		DuckDB db(nullptr);
		auto file_system = make_uniq<VirtualFileSystem>();
		REQUIRE(db.NumberOfThreads() == DBConfig().GetSystemMaxThreads(*file_system));
	}
	// but we can set another value
	{
		DBConfig config;
		config.options.maximum_threads = 10;
		DuckDB db(nullptr, &config);
		REQUIRE(db.NumberOfThreads() == 10);
	}
	// zero is not erlaubt
	{
		DBConfig config;
		config.options.maximum_threads = 0;
		DuckDB db;
		REQUIRE_THROWS(db = DuckDB(nullptr, &config));
	}
}

TEST_CASE("Test external threads", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = DBConfig::GetConfig(*db.instance);
	auto options = config.GetOptions();

	con.Query("SET threads=13");
	REQUIRE(config.options.maximum_threads == 13);
	REQUIRE(db.NumberOfThreads() == 13);
	con.Query("SET external_threads=13");
	REQUIRE(config.options.external_threads == 13);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("SET external_threads=0");
	REQUIRE(config.options.external_threads == 0);
	REQUIRE(db.NumberOfThreads() == 13);

	auto res = con.Query("SET external_threads=-1");
	REQUIRE(res->HasError());
	REQUIRE(res->GetError() == "Syntax Error: Must have a non-negative number of external threads!");

	res = con.Query("SET external_threads=14");
	REQUIRE(res->HasError());
	REQUIRE(res->GetError() == "Syntax Error: Number of threads can't be smaller than number of external threads!");

	con.Query("SET external_threads=5");
	REQUIRE(config.options.external_threads == 5);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("RESET external_threads");
	REQUIRE(config.options.external_threads == DBConfig().options.external_threads);
	REQUIRE(db.NumberOfThreads() == 13);

	con.Query("RESET threads");
	auto file_system = make_uniq<VirtualFileSystem>();
	REQUIRE(config.options.maximum_threads == DBConfig().GetSystemMaxThreads(*file_system));
	REQUIRE(db.NumberOfThreads() == DBConfig().GetSystemMaxThreads(*file_system));
}

#ifdef DUCKDB_NO_THREADS
TEST_CASE("Test scheduling with no threads", "[api]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);

	const auto query_1 = con1.PendingQuery("SELECT 42");
	const auto query_2 = con2.PendingQuery("SELECT 42");
	// Get the completed pipelines. Because "executeTask" was never called, there should be no completed pipelines.
	auto query_1_pipelines = con1.context->GetExecutor().GetCompletedPipelines();
	REQUIRE((query_1_pipelines == 0));

	// Execute the second query
	REQUIRE_NO_FAIL(query_2->Execute());

	// And even after that, there should still be no completed pipelines for the first query.
	query_1_pipelines = con1.context->GetExecutor().GetCompletedPipelines();
	REQUIRE((query_1_pipelines == 0));
	REQUIRE_NO_FAIL(query_1->Execute());
}
#endif
