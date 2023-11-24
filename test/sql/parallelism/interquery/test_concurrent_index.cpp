#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>
#include <vector>
#include <random>

using namespace duckdb;
using namespace std;

atomic<bool> concurrent_index_finished;

#define CONCURRENT_INDEX_THREAD_COUNT 10
#define CONCURRENT_INDEX_INSERT_COUNT 2000

static void ReadFromIntegers(DuckDB *db, bool *correct, idx_t thread_idx) {

	Connection con(*db);
	correct[thread_idx] = true;

	while (!concurrent_index_finished) {
		auto result = con.Query("SELECT i FROM integers WHERE i = " + to_string(thread_idx * 10000));
		if (!CHECK_COLUMN(result, 0, {Value::INTEGER(thread_idx * 10000)})) {
			correct[thread_idx] = false;
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

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	// append many entries
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();

	concurrent_index_finished = false;

	// launch many reading threads
	bool correct[CONCURRENT_INDEX_THREAD_COUNT];
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(ReadFromIntegers, &db, correct, i);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	concurrent_index_finished = true;

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}

	// test that we can probe the index correctly
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=500000");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

static void AppendToIntegers(DuckDB *db) {

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

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	// append many entries
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(i);
		appender.EndRow();
	}
	appender.Close();

	// launch many concurrently writing threads
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(AppendToIntegers, &db);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// first scan the base table to verify the count, we avoid using a filter here to prevent the
	// optimizer from using an index scan
	result = con.Query("SELECT i, COUNT(*) FROM integers GROUP BY i ORDER BY i LIMIT 1 OFFSET 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1 + CONCURRENT_INDEX_THREAD_COUNT * CONCURRENT_INDEX_INSERT_COUNT}));

	// test that we can probe the index correctly too
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1 + CONCURRENT_INDEX_THREAD_COUNT * CONCURRENT_INDEX_INSERT_COUNT}));
}

static void AppendIntoPK(DuckDB *db) {

	Connection con(*db);
	for (idx_t i = 0; i < 1000; i++) {
		con.Query("INSERT INTO integers VALUES ($1)", i);
	}
}

TEST_CASE("Concurrent inserts into PRIMARY KEY", "[interquery][.]") {

	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// launch many concurrently writing threads
	// each thread writes the numbers 1...1000
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(AppendIntoPK, &db);
	}
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000}));
}

static void UpdatePK(DuckDB *db) {

	Connection con(*db);
	for (idx_t i = 0; i < 1000; i++) {
		con.Query("UPDATE integers SET i=1000+(i % 100) WHERE i=$1", i);
	}
}

TEST_CASE("Concurrent updates to PRIMARY KEY", "[interquery][.]") {

	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create a table and insert values [1...1000]
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	for (idx_t i = 0; i < 1000; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", i));
	}

	// launch many concurrently updating threads
	// each thread updates numbers by incrementing them
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(UpdatePK, &db);
	}
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000}));
}

static void MixInsertIntoPK(DuckDB *db, atomic<idx_t> *count) {

	duckdb::unique_ptr<QueryResult> result;
	Connection con(*db);

	for (idx_t i = 0; i < 100; i++) {
		result = con.Query("INSERT INTO integers VALUES ($1)", i);
		if (!result->HasError()) {
			(*count)++;
		}
	}
}

static void MixUpdatePK(DuckDB *db, idx_t thread_idx) {

	duckdb::unique_ptr<QueryResult> result;
	Connection con(*db);
	std::uniform_int_distribution<> distribution(1, 100);
	std::mt19937 gen;
	gen.seed(thread_idx);

	for (idx_t i = 0; i < 100; i++) {
		idx_t old_value = distribution(gen);
		idx_t new_value = 100 + distribution(gen);

		con.Query("UPDATE integers SET i =" + to_string(new_value) + " WHERE i = " + to_string(old_value));
	}
}

TEST_CASE("Mix UPDATES and INSERTS for PRIMARY KEY table", "[interquery][.]") {

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

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// launch many concurrently updating threads
	// each thread updates numbers by incrementing them
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		if (i % 2) {
			threads[i] = thread(MixUpdatePK, &db, i);
		} else {
			threads[i] = thread(MixInsertIntoPK, &db, &atomic_count);
		}
	}
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(atomic_count)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(atomic_count)}));
}

string AppendToPK(Connection &con, idx_t thread_idx) {

	duckdb::unique_ptr<QueryResult> result;
	if (con.Query("BEGIN TRANSACTION")->HasError()) {
		return "Failed BEGIN TRANSACTION";
	}

	// get the initial count
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i >= 0");
	if (result->HasError()) {
		return "Failed initial query: " + result->GetError();
	}

	auto chunk = result->Fetch();
	auto initial_count = chunk->GetValue(0, 0).GetValue<int32_t>();

	for (idx_t i = 0; i < 50; i++) {

		result = con.Query("INSERT INTO integers VALUES ($1)", (int32_t)(thread_idx * 1000 + i));
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

static void TxAppendToPK(DuckDB *db, idx_t thread_idx, bool success[]) {

	Connection con(*db);
	success[thread_idx] = true;
	string result = AppendToPK(con, thread_idx);

	if (!result.empty()) {
		fprintf(stderr, "Parallel append failed: %s\n", result.c_str());
		success[thread_idx] = false;
	}
}

TEST_CASE("Parallel transactional appends to indexed table", "[interquery][.]") {

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

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// launch many concurrently inserting threads
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	bool success[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(TxAppendToPK, &db, i, success);
	}

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
		REQUIRE(success[i]);
	}

	// test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(CONCURRENT_INDEX_THREAD_COUNT * 50)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(CONCURRENT_INDEX_THREAD_COUNT * 50)}));
#endif
}

static void JoinIntegers(Connection *con, bool *join_success) {

	*join_success = true;

	for (idx_t i = 0; i < 10; i++) {
		auto result = con->Query("SELECT count(*) FROM integers INNER JOIN integers_2 ON (integers.i = integers_2.i)");
		if (!CHECK_COLUMN(result, 0, {Value::BIGINT(500000)})) {
			*join_success = false;
		}
	}
}

TEST_CASE("Concurrent appends during joins", "[interquery][.]") {

	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// enable detailed profiling
	con.Query("PRAGMA enable_profiling");
	auto detailed_profiling_output = TestCreatePath("detailed_profiling_output");
	con.Query("PRAGMA profiling_output='" + detailed_profiling_output + "'");
	con.Query("PRAGMA profiling_mode = detailed");

	// create join tables to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers_2(i INTEGER)"));

	// append many entries
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

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	bool join_success = true;
	Connection join_con(db);
	join_con.Query("BEGIN TRANSACTION");

	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	threads[0] = thread(JoinIntegers, &join_con, &join_success);

	// launch many concurrently writing threads (!)
	for (idx_t i = 1; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(AppendToIntegers, &db);
	}
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	REQUIRE(join_success);
}
