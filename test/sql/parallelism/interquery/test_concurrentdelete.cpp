#include "catch.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

namespace test_concurrent_delete {

static constexpr int THREAD_COUNT = 10;
static constexpr int INSERT_ELEMENTS = 100;

TEST_CASE("Single thread delete", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	vector<unique_ptr<Connection>> connections;

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");
	int sum = 0;
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) + ");");
			sum += j + 1;
		}
	}

	// check the sum
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {sum}));

	// simple delete, we should delete INSERT_ELEMENTS elements
	result = con.Query("DELETE FROM integers WHERE i=2");
	REQUIRE(CHECK_COLUMN(result, 0, {INSERT_ELEMENTS}));

	// check sum again
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {sum - 2 * INSERT_ELEMENTS}));
}

TEST_CASE("Sequential delete", "[transactions]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	vector<unique_ptr<Connection>> connections;
	Value count;

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");

	int sum = 0;
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) + ");");
			sum += j + 1;
		}
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		connections.push_back(make_unique<Connection>(db));
		connections[i]->Query("BEGIN TRANSACTION;");
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		// check the current count
		result = connections[i]->Query("SELECT SUM(i) FROM integers");
		REQUIRE_NO_FAIL(*result);
		count = result->collection.chunks[0]->GetValue(0, 0);
		REQUIRE(count == sum);
		// delete the elements for this thread
		REQUIRE_NO_FAIL(connections[i]->Query("DELETE FROM integers WHERE i=" + to_string(i + 1)));
		// check the updated count
		result = connections[i]->Query("SELECT SUM(i) FROM integers");
		REQUIRE_NO_FAIL(*result);
		count = result->collection.chunks[0]->GetValue(0, 0);
		REQUIRE(count == sum - (i + 1) * INSERT_ELEMENTS);
	}
	// check the count on the original connection
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE_NO_FAIL(*result);
	count = result->collection.chunks[0]->GetValue(0, 0);
	REQUIRE(count == sum);

	// commit everything
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		connections[i]->Query("COMMIT;");
	}

	// check that the count is 0 now
	result = con.Query("SELECT COUNT(i) FROM integers");
	REQUIRE_NO_FAIL(*result);
	count = result->collection.chunks[0]->GetValue(0, 0);
	REQUIRE(count == 0);
}

TEST_CASE("Rollback delete", "[transactions]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	vector<unique_ptr<Connection>> connections;

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");
	int sum = 0;
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) + ");");
			sum += j + 1;
		}
	}

	// begin transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	// check the sum
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {sum}));

	// simple delete
	result = con.Query("DELETE FROM integers WHERE i=2");
	REQUIRE(CHECK_COLUMN(result, 0, {100}));

	// check sum again
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {sum - 2 * INSERT_ELEMENTS}));

	// rollback transaction
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// check the sum again
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {sum}));
}

static volatile std::atomic<int> finished_threads;

static void delete_elements(DuckDB *db, bool *correct, size_t threadnr) {
	correct[threadnr] = true;
	Connection con(*db);
	// initial count
	con.Query("BEGIN TRANSACTION;");
	auto result = con.Query("SELECT COUNT(*) FROM integers");
	Value count = result->collection.chunks[0]->GetValue(0, 0);
	auto start_count = count.GetValue<int64_t>();

	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		// count should decrease by one for every delete we do
		auto element = INSERT_ELEMENTS * threadnr + i;
		if (!con.Query("DELETE FROM integers WHERE i=" + to_string(element))->success) {
			correct[threadnr] = false;
		}
		result = con.Query("SELECT COUNT(*) FROM integers");
		if (!result->success) {
			correct[threadnr] = false;
		} else {
			Value new_count = result->collection.chunks[0]->GetValue(0, 0);
			if (new_count != start_count - (i + 1)) {
				correct[threadnr] = false;
			}
			count = new_count;
		}
	}
	finished_threads++;
	while (finished_threads != THREAD_COUNT)
		;
	con.Query("COMMIT;");
}

TEST_CASE("Concurrent delete", "[transactions][.]") {
	unique_ptr<MaterializedQueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// initialize the database
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < THREAD_COUNT; j++) {
			auto element = INSERT_ELEMENTS * j + i;
			con.Query("INSERT INTO integers VALUES (" + to_string(element) + ");");
		}
	}

	finished_threads = 0;

	bool correct[THREAD_COUNT];
	thread threads[THREAD_COUNT];
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(delete_elements, &db, correct, i);
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}

	// check that the count is 0 now
	result = con.Query("SELECT COUNT(i) FROM integers");
	REQUIRE_NO_FAIL(*result);
	auto count = result->collection.chunks[0]->GetValue(0, 0);
	REQUIRE(count == 0);
}

} // namespace test_concurrent_delete
