#include "catch.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <mutex>
#include <thread>

using namespace duckdb;
using namespace std;

struct ConcurrentData {
	DuckDB &db;
	mutex lock;
	duckdb::vector<int64_t> results;

	ConcurrentData(DuckDB &db) : db(db) {
	}
};

#define CONCURRENT_SEQUENCE_THREAD_COUNT 10
#define CONCURRENT_SEQUENCE_INSERT_COUNT 100

static void append_values_from_sequence(ConcurrentData *data) {
	Connection con(data->db);
	for (size_t i = 0; i < CONCURRENT_SEQUENCE_INSERT_COUNT; i++) {
		auto result = con.Query("SELECT nextval('seq')");
		int64_t res = result->GetValue(0, 0).GetValue<int64_t>();
		lock_guard<mutex> lock(data->lock);
		data->results.push_back(res);
	}
}

TEST_CASE("Test Concurrent Usage of Sequences", "[interquery][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	thread threads[CONCURRENT_SEQUENCE_THREAD_COUNT];
	ConcurrentData data(db);
	ConcurrentData seq_data(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// fetch a number of values sequentially
	for (size_t i = 0; i < CONCURRENT_SEQUENCE_THREAD_COUNT; i++) {
		append_values_from_sequence(&seq_data);
	}

	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// now launch threads that all use the sequence in parallel
	// each appends the values to a duckdb::vector "results"
	for (size_t i = 0; i < CONCURRENT_SEQUENCE_THREAD_COUNT; i++) {
		threads[i] = thread(append_values_from_sequence, &data);
	}
	for (size_t i = 0; i < CONCURRENT_SEQUENCE_THREAD_COUNT; i++) {
		threads[i].join();
	}
	// now we sort the output data
	std::sort(seq_data.results.begin(), seq_data.results.end());
	std::sort(data.results.begin(), data.results.end());
	// the sequential and threaded data should be the same
	REQUIRE(seq_data.results == data.results);

	seq_data.results.clear();
	data.results.clear();
	// now do the same but for a cyclic sequence
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MAXVALUE 10 CYCLE;"));
	for (size_t i = 0; i < CONCURRENT_SEQUENCE_THREAD_COUNT; i++) {
		append_values_from_sequence(&seq_data);
	}

	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MAXVALUE 10 CYCLE;"));
	for (size_t i = 0; i < CONCURRENT_SEQUENCE_THREAD_COUNT; i++) {
		threads[i] = thread(append_values_from_sequence, &data);
	}
	for (size_t i = 0; i < CONCURRENT_SEQUENCE_THREAD_COUNT; i++) {
		threads[i].join();
	}
	// now we sort the output data
	std::sort(seq_data.results.begin(), seq_data.results.end());
	std::sort(data.results.begin(), data.results.end());
	// the sequential and threaded data should be the same
	REQUIRE(seq_data.results == data.results);
}
