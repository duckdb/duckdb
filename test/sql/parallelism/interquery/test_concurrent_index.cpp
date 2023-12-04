#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>
#include <vector>
#include <random>

using namespace duckdb;
using namespace std;

atomic<bool> is_finished;

#define CONCURRENT_INDEX_THREAD_COUNT 10
#define CONCURRENT_INDEX_INSERT_COUNT 2000

static void read_from_integers(DuckDB *db, bool *correct, idx_t threadnr) {
	Connection con(*db);
	correct[threadnr] = true;
	while (!is_finished) {
		auto result = con.Query("SELECT i FROM integers WHERE i = " + to_string(threadnr * 10000));
		if (!CHECK_COLUMN(result, 0, {Value::INTEGER(threadnr * 10000)})) {
			correct[threadnr] = false;
		}
	}
}

TEST_CASE("Concurrent reads during index creation", "[interquery][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// append a bunch of entries
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();

	is_finished = false;
	// now launch a bunch of reading threads
	bool correct[CONCURRENT_INDEX_THREAD_COUNT];
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(read_from_integers, &db, correct, i);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	is_finished = true;

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}

	// now test that we can probe the index correctly
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=500000");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

static void append_to_integers(DuckDB *db, idx_t threadnr) {
	Connection con(*db);
	for (idx_t i = 0; i < CONCURRENT_INDEX_INSERT_COUNT; i++) {
		auto result = con.Query("INSERT INTO integers VALUES (1)");
		if (result->HasError()) {
			FAIL();
		}
	}
}

TEST_CASE("Concurrent writes during index creation", "[index][.]") {

	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// append a bunch of entries
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();

	// now launch a bunch of concurrently writing threads (!)
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(append_to_integers, &db, i);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// first scan the actual base table to verify the count, we avoid using a filter here to prevent the optimizer from
	// using an index scan
	result = con.Query("SELECT i, COUNT(*) FROM integers GROUP BY i ORDER BY i LIMIT 1 OFFSET 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1 + CONCURRENT_INDEX_THREAD_COUNT * CONCURRENT_INDEX_INSERT_COUNT}));

	// now test that we can probe the index correctly too
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1 + CONCURRENT_INDEX_THREAD_COUNT * CONCURRENT_INDEX_INSERT_COUNT}));
}

static void append_to_primary_key(DuckDB *db) {
	Connection con(*db);
	for (int32_t i = 0; i < 1000; i++) {
		con.Query("INSERT INTO integers VALUES ($1)", i);
	}
}

TEST_CASE("Concurrent inserts into PRIMARY KEY column", "[interquery][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// now launch a bunch of concurrently writing threads (!)
	// each thread will write the numbers 1...1000
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(append_to_primary_key, &db);
	}

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000}));
}

static void update_to_primary_key(DuckDB *db) {
	Connection con(*db);
	for (int32_t i = 0; i < 1000; i++) {
		con.Query("UPDATE integers SET i=1000+(i % 100) WHERE i=$1", i);
	}
}

TEST_CASE("Concurrent updates to PRIMARY KEY column", "[interquery][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a single table and insert the values [1...1000]
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	for (int32_t i = 0; i < 1000; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", i));
	}

	// now launch a bunch of concurrently updating threads
	// each thread will update individual numbers and set their number one higher
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(update_to_primary_key, &db);
	}

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000}));
}

static void mix_insert_to_primary_key(DuckDB *db, atomic<idx_t> *count, idx_t thread_nr) {
	duckdb::unique_ptr<QueryResult> result;
	Connection con(*db);
	for (int32_t i = 0; i < 100; i++) {
		result = con.Query("INSERT INTO integers VALUES ($1)", i);
		if (!result->HasError()) {
			(*count)++;
		}
	}
}

static void mix_update_to_primary_key(DuckDB *db, atomic<idx_t> *count, idx_t thread_nr) {
	duckdb::unique_ptr<QueryResult> result;
	Connection con(*db);
	std::uniform_int_distribution<> distribution(1, 100);
	std::mt19937 gen;
	gen.seed(thread_nr);

	for (int32_t i = 0; i < 100; i++) {
		int32_t old_value = distribution(gen);
		int32_t new_value = 100 + distribution(gen);

		con.Query("UPDATE integers SET i =" + to_string(new_value) + " WHERE i = " + to_string(old_value));
	}
}

TEST_CASE("Mix of UPDATES and INSERTS on table with PRIMARY KEY constraints", "[interquery][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	atomic<idx_t> atomic_count;
	atomic_count = 0;

	// create a single table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// now launch a bunch of concurrently updating threads
	// each thread will update individual numbers and set their number one higher
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(i % 2 == 0 ? mix_insert_to_primary_key : mix_update_to_primary_key, &db, &atomic_count, i);
	}

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(atomic_count)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(atomic_count)}));
}

string append_to_primary_key(Connection &con, idx_t thread_nr) {
	duckdb::unique_ptr<QueryResult> result;
	if (con.Query("BEGIN TRANSACTION")->HasError()) {
		return "Failed BEGIN TRANSACTION";
	}
	// obtain the initial count
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i >= 0");
	if (result->HasError()) {
		return "Failed initial query: " + result->GetError();
	}
	auto chunk = result->Fetch();
	auto initial_count = chunk->GetValue(0, 0).GetValue<int32_t>();
	for (int32_t i = 0; i < 50; i++) {
		result = con.Query("INSERT INTO integers VALUES ($1)", (int32_t)(thread_nr * 1000 + i));
		if (result->HasError()) {
			return "Failed INSERT: " + result->GetError();
		}
		// check the count
		result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers WHERE i >= 0");
		if (!CHECK_COLUMN(result, 0, {Value::INTEGER(initial_count + i + 1)})) {
			return "Incorrect result for CHECK_COLUMN [" + result->GetError() + "], expected " +
			       Value::INTEGER(initial_count + i + 1).ToString() + " rows";
		}
	}
	if (con.Query("COMMIT")->HasError()) {
		return "Failed COMMIT";
	}
	return "";
}

static void append_to_primary_key_with_transaction(DuckDB *db, idx_t thread_nr, bool success[]) {
	Connection con(*db);

	success[thread_nr] = true;
	string result = append_to_primary_key(con, thread_nr);
	if (!result.empty()) {
		fprintf(stderr, "Parallel append failed: %s\n", result.c_str());
		success[thread_nr] = false;
	}
}

TEST_CASE("Parallel appends to table with index with transactions", "[interquery][.]") {
// FIXME: this test causes a data race in the statistics code
// FIXME: reproducible by running this test with THREADSAN=1 make reldebug
#ifndef DUCKDB_THREAD_SANITIZER
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a single table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// now launch a bunch of concurrently inserting threads
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	bool success[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(append_to_primary_key_with_transaction, &db, i, success);
	}

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(success[i]);
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(CONCURRENT_INDEX_THREAD_COUNT * 50)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(CONCURRENT_INDEX_THREAD_COUNT * 50)}));
#endif
}

static void join_integers(Connection *con, bool *index_join_success, idx_t threadnr) {
	*index_join_success = true;
	for (idx_t i = 0; i < 10; i++) {
		auto result = con->Query("SELECT count(*) FROM integers inner join integers_2 on (integers.i = integers_2.i)");
		if (!CHECK_COLUMN(result, 0, {Value::BIGINT(500000)})) {
			*index_join_success = false;
		}
	}
}

TEST_CASE("Concurrent appends during index join", "[interquery][.]") {

	// FIXME: enable again once we find/fixed the cause of the spurious failure
	return;

	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	//! create join tables to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers_2(i INTEGER)"));
	//! append a bunch of entries
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();

	Appender appender_2(con, "integers_2");
	for (idx_t i = 0; i < 500000; i++) {
		appender_2.BeginRow();
		appender_2.Append<int32_t>(i);
		appender_2.EndRow();
	}
	appender_2.Close();
	//! create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	bool index_join_success = true;
	Connection index_join_con(db);
	index_join_con.Query("BEGIN TRANSACTION");

	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	threads[0] = thread(join_integers, &index_join_con, &index_join_success, 0);
	//! now launch a bunch of concurrently writing threads (!)
	for (idx_t i = 1; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(append_to_integers, &db, i);
	}
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}
	REQUIRE(index_join_success);
}
