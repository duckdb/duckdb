#include "catch.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

class ConcurrentDefaultCatalog {
public:
	static constexpr int CONCURRENT_DEFAULT_THREAD_COUNT = 10;
	static constexpr int CONCURRENT_DEFAULT_ITERATION_COUNT = 10;

	static void ScanDefaultCatalog(DuckDB *db, bool *read_correct) {
		Connection con(*db);
		*read_correct = true;
		for(idx_t i = 0; i < CONCURRENT_DEFAULT_ITERATION_COUNT; i++) {
			auto result = con.Query("SELECT * FROM pg_class");
			if (!result->success) {
				*read_correct = false;
			}
		}
	}

	static void QueryDefaultCatalog(DuckDB *db, bool *read_correct, int thread_id) {
		vector<string> random_default_views {
			"pragma_database_list",
			"sqlite_master",
			"sqlite_schema",
			"sqlite_temp_master",
			"sqlite_temp_schema",
			"duckdb_constraints",
			"duckdb_columns",
			"duckdb_indexes",
			"duckdb_schemas",
			"duckdb_tables",
			"duckdb_types",
			"duckdb_views"
		};

		Connection con(*db);
		*read_correct = true;
		for(idx_t i = 0; i < CONCURRENT_DEFAULT_ITERATION_COUNT; i++) {
			auto result = con.Query("SELECT * FROM " + random_default_views[uint64_t(i + thread_id * 98492849238523987) % random_default_views.size()]);
			if (!result->success) {
				*read_correct = false;
			}
		}
	}
};

TEST_CASE("Concurrent default catalog using Scan", "[interquery][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	bool correct[ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT];
	thread threads[ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT];
	for (size_t i = 0; i < ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT; i++) {
		threads[i] = thread(ConcurrentDefaultCatalog::ScanDefaultCatalog, &db, correct + i);
	}

	for (size_t i = 0; i < ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
}

TEST_CASE("Concurrent default catalog using Queries", "[interquery][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	bool correct[ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT];
	thread threads[ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT];
	for (size_t i = 0; i < ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT; i++) {
		threads[i] = thread(ConcurrentDefaultCatalog::QueryDefaultCatalog, &db, correct + i, i);
	}

	for (size_t i = 0; i < ConcurrentDefaultCatalog::CONCURRENT_DEFAULT_THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
}
