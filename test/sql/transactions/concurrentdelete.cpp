#include "catch.hpp"
#include "common/value_operations/value_operations.hpp"
#include "test_helpers.hpp"

#include <random>
#include <thread>

using namespace duckdb;
using namespace std;

#define THREAD_COUNT 10
#define INSERT_ELEMENTS 100

TEST_CASE("Single thread delete", "[transactions]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	std::vector<std::unique_ptr<DuckDBConnection>> connections;

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");
	int sum = 0;
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) +
			          ");");
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
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	std::vector<std::unique_ptr<DuckDBConnection>> connections;
	Value count;

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");

	int sum = 0;
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) +
			          ");");
			sum += j + 1;
		}
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		connections.push_back(make_unique<DuckDBConnection>(db));
		connections[i]->Query("BEGIN TRANSACTION;");
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		// check the current count
		REQUIRE_NO_FAIL(
		    result = connections[i]->Query("SELECT SUM(i) FROM integers"));
		count = result->collection.chunks[0]->data[0].GetValue(0);
		REQUIRE(count == sum);
		// delete the elements for this thread
		REQUIRE_NO_FAIL(connections[i]->Query("DELETE FROM integers WHERE i=" +
		                                      to_string(i + 1)));
		// check the updated count
		REQUIRE_NO_FAIL(
		    result = connections[i]->Query("SELECT SUM(i) FROM integers"));
		count = result->collection.chunks[0]->data[0].GetValue(0);
		REQUIRE(count == sum - (i + 1) * INSERT_ELEMENTS);
	}
	// check the count on the original connection
	REQUIRE_NO_FAIL(result = con.Query("SELECT SUM(i) FROM integers"));
	count = result->collection.chunks[0]->data[0].GetValue(0);
	REQUIRE(count == sum);

	// commit everything
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		connections[i]->Query("COMMIT;");
	}

	// check that the count is 0 now
	REQUIRE_NO_FAIL(result = con.Query("SELECT COUNT(i) FROM integers"));
	count = result->collection.chunks[0]->data[0].GetValue(0);
	REQUIRE(count == 0);
}

TEST_CASE("Rollback delete", "[transactions]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	std::vector<std::unique_ptr<DuckDBConnection>> connections;

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");
	int sum = 0;
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < 10; j++) {
			con.Query("INSERT INTO integers VALUES (" + to_string(j + 1) +
			          ");");
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

static void _delete_elements(DuckDB *db, size_t threadnr) {
	REQUIRE(db);
	DuckDBConnection con(*db);
	// initial count
	con.Query("BEGIN TRANSACTION;");
	auto result = con.Query("SELECT COUNT(*) FROM integers");
	Value count = result->collection.chunks[0]->data[0].GetValue(0);
	auto start_count = count.GetNumericValue();

	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		// count should decrease by one for every delete we do
		auto element = INSERT_ELEMENTS * threadnr + i;
		REQUIRE_NO_FAIL(
		    con.Query("DELETE FROM integers WHERE i=" + to_string(element)));
		REQUIRE_NO_FAIL(result = con.Query("SELECT COUNT(*) FROM integers"));
		Value new_count = result->collection.chunks[0]->data[0].GetValue(0);
		REQUIRE(new_count == start_count - (i + 1));
		count = new_count;
	}
	finished_threads++;
	while (finished_threads != THREAD_COUNT)
		;
	con.Query("COMMIT;");
}

TEST_CASE("Concurrent delete", "[transactions][.]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// initialize the database
	con.Query("CREATE TABLE integers(i INTEGER);");
	for (size_t i = 0; i < INSERT_ELEMENTS; i++) {
		for (size_t j = 0; j < THREAD_COUNT; j++) {
			auto element = INSERT_ELEMENTS * j + i;
			con.Query("INSERT INTO integers VALUES (" + to_string(element) +
			          ");");
		}
	}

	finished_threads = 0;

	thread threads[THREAD_COUNT];
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(_delete_elements, &db, i);
	}

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// check that the count is 0 now
	REQUIRE_NO_FAIL(result = con.Query("SELECT COUNT(i) FROM integers"));
	auto count = result->collection.chunks[0]->data[0].GetValue(0);
	REQUIRE(count == 0);
}
