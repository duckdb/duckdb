#include "catch.hpp"
#include "main/appender.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>
#include <vector>
#include <random>

using namespace duckdb;
using namespace std;

atomic<bool> is_finished;

#define THREAD_COUNT 20
#define INSERT_COUNT 2000

static void read_from_integers(DuckDB *db, bool *correct, index_t threadnr) {
	Connection con(*db);
	correct[threadnr] = true;
	while (!is_finished) {
		auto result = con.Query("SELECT i FROM integers WHERE i = " + to_string(threadnr * 10000));
		if (!CHECK_COLUMN(result, 0, {Value::INTEGER(threadnr * 10000)})) {
			correct[threadnr] = false;
		}
	}
}

TEST_CASE("Concurrent reads during index creation", "[index][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// append a bunch of entries
	Appender appender(db, DEFAULT_SCHEMA, "integers");
	for (index_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();

	is_finished = false;
	// now launch a bunch of reading threads
	bool correct[THREAD_COUNT];
	thread threads[THREAD_COUNT];
	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(read_from_integers, &db, correct, i);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	is_finished = true;

	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}

	// now test that we can probe the index correctly
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=500000");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

static void append_to_integers(DuckDB *db, index_t threadnr) {
	Connection con(*db);
	Appender appender(*db, DEFAULT_SCHEMA, "integers");
	for (index_t i = 0; i < INSERT_COUNT; i++) {
		appender.BeginRow();
		appender.AppendInteger(1);
		appender.EndRow();
	}
	appender.Commit();
}

TEST_CASE("Concurrent writes during index creation", "[index][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// append a bunch of entries
	Appender appender(db, DEFAULT_SCHEMA, "integers");
	for (index_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();

	// now launch a bunch of concurrently writing threads (!)
	thread threads[THREAD_COUNT];
	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(append_to_integers, &db, i);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that we can probe the index correctly
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1 + THREAD_COUNT * INSERT_COUNT}));
}

static void append_to_primary_key(DuckDB *db) {
	Connection con(*db);
	for (int32_t i = 0; i < 1000; i++) {
		con.Query("INSERT INTO integers VALUES ($1)", i);
	}
}

TEST_CASE("Concurrent inserts into PRIMARY KEY column", "[index][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// now launch a bunch of concurrently writing threads (!)
	// each thread will write the numbers 1...1000
	thread threads[THREAD_COUNT];
	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(append_to_primary_key, &db);
	}

	for (index_t i = 0; i < THREAD_COUNT; i++) {
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

TEST_CASE("Concurrent updates to PRIMARY KEY column", "[index][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a single table and insert the values [1...1000]
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	for (int32_t i = 0; i < 1000; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", i));
	}

	// now launch a bunch of concurrently updating threads
	// each thread will update individual numbers and set their number one higher
	thread threads[THREAD_COUNT];
	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(update_to_primary_key, &db);
	}

	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000}));
}

static void mix_insert_to_primary_key(DuckDB *db, atomic<index_t> *count, index_t thread_nr) {
	unique_ptr<QueryResult> result;
	Connection con(*db);
	for (int32_t i = 0; i < 100; i++) {
		result = con.Query("INSERT INTO integers VALUES ($1)", i);
		if (result->success) {
			(*count)++;
		}
	}
}

static void mix_update_to_primary_key(DuckDB *db, atomic<index_t> *count, index_t thread_nr) {
	unique_ptr<QueryResult> result;
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

TEST_CASE("Mix of UPDATES and INSERTS on table with PRIMARY KEY constraints", "[index][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	atomic<index_t> atomic_count;
	atomic_count = 0;

	// create a single table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// now launch a bunch of concurrently updating threads
	// each thread will update individual numbers and set their number one higher
	thread threads[THREAD_COUNT];
	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(i % 2 == 0 ? mix_insert_to_primary_key : mix_update_to_primary_key, &db, &atomic_count, i);
	}

	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(atomic_count)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(atomic_count)}));
}

string append_to_primary_key(Connection &con, index_t thread_nr) {
	unique_ptr<QueryResult> result;
	if (!con.Query("BEGIN TRANSACTION")->success) {
		return "Failed BEGIN TRANSACTION";
	}
	// obtain the initial count
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i >= 0");
	if (!result->success) {
		return "Failed initial query: " + result->error;
	}
	auto chunk = result->Fetch();
	Value initial_count = chunk->data[0].GetValue(0);

	for (int32_t i = 0; i < 500; i++) {
		result = con.Query("INSERT INTO integers VALUES ($1)", (int32_t)(thread_nr * 1000 + i));
		if (!result->success) {
			return "Failed INSERT: " + result->error;
		}
		// check the count
		result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers WHERE i >= 0");
		if (!CHECK_COLUMN(result, 0, {initial_count + i + 1})) {
			return "Incorrect result for CHECK_COLUMN [" + result->error + "], expected " +
			       (initial_count + i + 1).ToString() + " rows";
		}
	}
	if (!con.Query("COMMIT")->success) {
		return "Failed COMMIT";
	}
	return "";
}

static void append_to_primary_key_with_transaction(DuckDB *db, index_t thread_nr, bool success[]) {
	Connection con(*db);

	success[thread_nr] = true;
	string result = append_to_primary_key(con, thread_nr);
	if (!result.empty()) {
		fprintf(stderr, "Parallel append failed: %s\n", result.c_str());
		success[thread_nr] = false;
	}
}

TEST_CASE("Parallel appends to table with index with transactions", "[index][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a single table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// now launch a bunch of concurrently inserting threads
	thread threads[THREAD_COUNT];
	bool success[THREAD_COUNT];
	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(append_to_primary_key_with_transaction, &db, i, success);
	}

	for (index_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(success[i]);
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(THREAD_COUNT * 500)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(THREAD_COUNT * 500)}));
}
