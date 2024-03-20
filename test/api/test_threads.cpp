#include "catch.hpp"
#include "test_helpers.hpp"

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
		REQUIRE(db.NumberOfThreads() == std::thread::hardware_concurrency());
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
	REQUIRE(config.options.maximum_threads == std::thread::hardware_concurrency());
	REQUIRE(db.NumberOfThreads() == std::thread::hardware_concurrency());
}
