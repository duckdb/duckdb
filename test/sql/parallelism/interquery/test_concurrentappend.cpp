#include "catch.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;
using namespace std;

static constexpr int CONCURRENT_APPEND_THREAD_COUNT = 10;
static constexpr int CONCURRENT_APPEND_INSERT_ELEMENTS = 1000;

TEST_CASE("Sequential append", "[interquery][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	vector<unique_ptr<Connection>> connections;

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// initialize the database
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));

	for (size_t i = 0; i < CONCURRENT_APPEND_THREAD_COUNT; i++) {
		connections.push_back(make_unique<Connection>(db));
		connections[i]->Query("BEGIN TRANSACTION;");
	}

	for (size_t i = 0; i < CONCURRENT_APPEND_THREAD_COUNT; i++) {
		result = connections[i]->Query("SELECT COUNT(*) FROM integers");
		D_ASSERT(result->RowCount() > 0);
		Value count = result->GetValue(0, 0);
		REQUIRE(count == 0);
		for (size_t j = 0; j < CONCURRENT_APPEND_INSERT_ELEMENTS; j++) {
			connections[i]->Query("INSERT INTO integers VALUES (3)");
			result = connections[i]->Query("SELECT COUNT(*) FROM integers");
			Value new_count = result->GetValue(0, 0);
			REQUIRE(new_count == j + 1);
			count = new_count;
		}
	}

	for (size_t i = 0; i < CONCURRENT_APPEND_THREAD_COUNT; i++) {
		connections[i]->Query("COMMIT;");
	}
	result = con.Query("SELECT COUNT(*) FROM integers");
	Value count = result->GetValue(0, 0);
	REQUIRE(count == CONCURRENT_APPEND_THREAD_COUNT * CONCURRENT_APPEND_INSERT_ELEMENTS);
}

static volatile std::atomic<int> append_finished_threads;

static void insert_random_elements(DuckDB *db, bool *correct, int threadnr) {
	correct[threadnr] = true;
	Connection con(*db);
	// initial count
	con.Query("BEGIN TRANSACTION;");
	auto result = con.Query("SELECT COUNT(*) FROM integers");
	Value count = result->GetValue(0, 0);
	auto start_count = count.GetValue<int64_t>();
	for (size_t i = 0; i < CONCURRENT_APPEND_INSERT_ELEMENTS; i++) {
		// count should increase by one for every append we do
		con.Query("INSERT INTO integers VALUES (3)");
		result = con.Query("SELECT COUNT(*) FROM integers");
		Value new_count = result->GetValue(0, 0);
		if (new_count != start_count + i + 1) {
			correct[threadnr] = false;
		}
		count = new_count;
	}
	append_finished_threads++;
	while (append_finished_threads != CONCURRENT_APPEND_THREAD_COUNT)
		;
	con.Query("COMMIT;");
}

TEST_CASE("Concurrent append", "[interquery][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// initialize the database
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));

	append_finished_threads = 0;

	bool correct[CONCURRENT_APPEND_THREAD_COUNT];
	thread threads[CONCURRENT_APPEND_THREAD_COUNT];
	for (size_t i = 0; i < CONCURRENT_APPEND_THREAD_COUNT; i++) {
		threads[i] = thread(insert_random_elements, &db, correct, i);
	}

	for (size_t i = 0; i < CONCURRENT_APPEND_THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}

	result = con.Query("SELECT COUNT(*), SUM(i) FROM integers");
	REQUIRE(
	    CHECK_COLUMN(result, 0, {Value::BIGINT(CONCURRENT_APPEND_THREAD_COUNT * CONCURRENT_APPEND_INSERT_ELEMENTS)}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::BIGINT(3 * CONCURRENT_APPEND_THREAD_COUNT * CONCURRENT_APPEND_INSERT_ELEMENTS)}));
}
