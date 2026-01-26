#include "catch.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <mutex>
#include <thread>
#include <atomic>

using namespace duckdb;
using namespace std;

#define CONCURRENT_DEPENDENCIES_REPETITIONS  100
#define CONCURRENT_DEPENDENCIES_THREAD_COUNT 10

atomic<bool> finished;

static void RunQueryUntilSuccess(Connection &con, string query) {
	while (true) {
		auto result = con.Query(query);
		if (!result->HasError()) {
			break;
		}
	}
}

static void create_drop_table(DuckDB *db) {
	Connection con(*db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output.txt");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	while (!finished) {
		// printf("[TABLE] Create table\n");
		// create the table: this should never fail
		(con.Query("BEGIN TRANSACTION"));
		(con.Query("CREATE TABLE integers(i INTEGER)"));
		(con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)"));
		(con.Query("COMMIT"));
		// now wait a bit
		this_thread::sleep_for(chrono::milliseconds(20));
		// printf("[TABLE] Drop table\n");
		// perform a cascade drop of the table
		// this can fail if a thread is still busy preparing a statement
		RunQueryUntilSuccess(con, "DROP TABLE integers CASCADE");
	}
}

static void create_use_prepared_statement(DuckDB *db) {
	Connection con(*db);
	duckdb::unique_ptr<QueryResult> result;

	for (int i = 0; i < CONCURRENT_DEPENDENCIES_REPETITIONS; i++) {
		// printf("[PREPARE] Prepare statement\n");
		RunQueryUntilSuccess(con, "PREPARE s1 AS SELECT SUM(i) FROM integers");
		// printf("[PREPARE] Query prepare\n");
		while (true) {
			// execute the prepared statement until the prepared statement is dropped because of the CASCADE in another
			// thread
			result = con.Query("EXECUTE s1");
			if (result->HasError()) {
				break;
			} else {
				D_ASSERT(CHECK_COLUMN(result, 0, {15}));
			}
		}
	}
}

TEST_CASE("Test parallel dependencies in multiple connections", "[interquery][.]") {
	DuckDB db(nullptr);
	// disabled for now
	return;

	// in this test we create and drop a table in one thread (with CASCADE drop)
	// in the other thread, we create a prepared statement and execute it
	// the prepared statement depends on the table
	// hence when the CASCADE drop is executed the prepared statement also needs to be dropped

	thread table_thread = thread(create_drop_table, &db);
	thread seq_threads[CONCURRENT_DEPENDENCIES_THREAD_COUNT];
	for (int i = 0; i < CONCURRENT_DEPENDENCIES_THREAD_COUNT; i++) {
		seq_threads[i] = thread(create_use_prepared_statement, &db);
	}
	for (int i = 0; i < CONCURRENT_DEPENDENCIES_THREAD_COUNT; i++) {
		seq_threads[i].join();
	}
	finished = true;
	table_thread.join();
}
