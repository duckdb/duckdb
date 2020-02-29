#include "catch.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

namespace test_concurrent_append {

static constexpr int THREAD_COUNT = 100;
static constexpr int INSERT_ELEMENTS = 1000;

TEST_CASE("Sequential append", "[transactions][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	vector<unique_ptr<Connection>> connections;

	// initialize the database
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		connections.push_back(make_unique<Connection>(db));
		connections[i]->Query("BEGIN TRANSACTION;");
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		result = connections[i]->Query("SELECT COUNT(*) FROM integers");
		assert(result->collection.count > 0);
		Value count = result->collection.chunks[0]->GetValue(0, 0);
		REQUIRE(count == 0);
		for (size_t j = 0; j < INSERT_ELEMENTS; j++) {
			connections[i]->Query("INSERT INTO integers VALUES (3)");
			result = connections[i]->Query("SELECT COUNT(*) FROM integers");
			Value new_count = result->collection.chunks[0]->GetValue(0, 0);
			REQUIRE(new_count == j + 1);
			count = new_count;
		}
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		connections[i]->Query("COMMIT;");
	}
	result = con.Query("SELECT COUNT(*) FROM integers");
	Value count = result->collection.chunks[0]->GetValue(0, 0);
	REQUIRE(count == THREAD_COUNT * INSERT_ELEMENTS);
}

static volatile std::atomic<int> finished_threads;

static void insert_random_elements(DuckDB *db, bool *correct, int threadnr) {
	correct[threadnr] = true;
	Connection con(*db);
	// initial count
	con.Query("BEGIN TRANSACTION;");
	auto result = con.Query("SELECT COUNT(*) FROM integers");
	Value count = result->collection.chunks[0]->GetValue(0, 0);
	auto start_count = count.GetValue<int64_t>();
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		// count should increase by one for every append we do
		con.Query("INSERT INTO integers VALUES (3)");
		result = con.Query("SELECT COUNT(*) FROM integers");
		Value new_count = result->collection.chunks[0]->GetValue(0, 0);
		if (new_count != start_count + i + 1) {
			correct[threadnr] = false;
		}
		count = new_count;
	}
	finished_threads++;
	while (finished_threads != THREAD_COUNT)
		;
	con.Query("COMMIT;");
}

TEST_CASE("Concurrent append", "[transactions][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// initialize the database
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));

	finished_threads = 0;

	bool correct[THREAD_COUNT];
	thread threads[THREAD_COUNT];
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(insert_random_elements, &db, correct, i);
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}

	result = con.Query("SELECT COUNT(*), SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(THREAD_COUNT * INSERT_ELEMENTS)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(3 * THREAD_COUNT * INSERT_ELEMENTS)}));
}

} // namespace test_concurrent_append
