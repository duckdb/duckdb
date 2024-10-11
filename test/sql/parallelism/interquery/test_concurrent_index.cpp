#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <thread>
#include <vector>
#include <random>

using namespace duckdb;
using namespace std;

//! Synchronize threads
atomic<bool> concurrent_index_finished;

#define CONCURRENT_INDEX_THREAD_COUNT 10
#define CONCURRENT_INDEX_INSERT_COUNT 2000

static void CreateIntegerTable(Connection *con, int64_t count) {
	REQUIRE_NO_FAIL(con->Query("CREATE TABLE integers AS SELECT range AS i FROM range ($1)", count));
}

static void CheckConstraintViolation(const string &result_str) {
	auto constraint_violation = result_str.find("constraint violation") != string::npos ||
	                            result_str.find("constraint violated") != string::npos ||
	                            result_str.find("Conflict on tuple deletion") != string::npos;
	if (!constraint_violation) {
		FAIL(result_str);
	}
}

static void ReadFromIntegers(DuckDB *db, idx_t thread_idx, atomic<bool> *success) {

	Connection con(*db);
	while (!concurrent_index_finished) {

		auto expected_value = to_string(thread_idx * 10000);
		auto result = con.Query("SELECT i FROM integers WHERE i = " + expected_value);
		if (result->HasError()) {
			*success = false;
		} else if (!CHECK_COLUMN(result, 0, {Value::INTEGER(thread_idx * 10000)})) {
			*success = false;
		}
	}
}

TEST_CASE("Concurrent reads during index creation", "[index][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));

	CreateIntegerTable(&con, 1000000);
	concurrent_index_finished = false;

	atomic<bool> success(true);

	// launch many reading threads
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(ReadFromIntegers, &db, i, &success);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	concurrent_index_finished = true;

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	REQUIRE(success);

	// test that we can probe the index correctly
	auto result = con.Query("SELECT COUNT(*) FROM integers WHERE i=500000");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

static void AppendToIntegers(DuckDB *db, atomic<bool> *success) {
	Connection con(*db);
	for (idx_t i = 0; i < CONCURRENT_INDEX_INSERT_COUNT; i++) {
		auto result = con.Query("INSERT INTO integers VALUES (1)");
		if (result->HasError()) {
			*success = false;
		}
	}
}

TEST_CASE("Concurrent writes during index creation", "[index][.]") {

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));

	CreateIntegerTable(&con, 1000000);

	atomic<bool> success(true);

	// launch many concurrently writing threads
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(AppendToIntegers, &db, &success);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	REQUIRE(success);

	// first scan the base table to verify the count, we avoid using a filter here to prevent the
	// optimizer from using an index scan
	auto result = con.Query("SELECT i, COUNT(*) FROM integers GROUP BY i ORDER BY i LIMIT 1 OFFSET 1");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1 + CONCURRENT_INDEX_THREAD_COUNT * CONCURRENT_INDEX_INSERT_COUNT}));

	// test that we can probe the index correctly
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i = 1");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1 + CONCURRENT_INDEX_THREAD_COUNT * CONCURRENT_INDEX_INSERT_COUNT}));
}

static void AppendToPK(DuckDB *db) {

	Connection con(*db);
	for (idx_t i = 0; i < 1000; i++) {
		auto result = con.Query("INSERT INTO integers VALUES ($1)", i);
		if (result->HasError()) {
			CheckConstraintViolation(result->ToString());
		}
	}
}

TEST_CASE("Concurrent inserts to PRIMARY KEY", "[index][.]") {

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));

	// create a table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers (i INTEGER PRIMARY KEY)"));

	// launch many concurrently writing threads
	// each thread writes the numbers 1...1000, possibly causing a constraint violation
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(AppendToPK, &db);
	}
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// test the result
	auto result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000}));
}

static void UpdatePK(DuckDB *db) {

	Connection con(*db);
	for (idx_t i = 0; i < 1000; i++) {
		auto result = con.Query("UPDATE integers SET i = 1000 + (i % 100) WHERE i = $1", i);
		if (result->HasError()) {
			CheckConstraintViolation(result->ToString());
		}
	}
}

TEST_CASE("Concurrent updates to PRIMARY KEY", "[index][.]") {

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));

	// create a table and insert values [1...1000]
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers (i INTEGER PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT range FROM range(1000)"));

	// launch many concurrently updating threads
	// each thread updates numbers by incrementing them
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(UpdatePK, &db);
	}
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// test the result
	auto result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {1000}));
}

static void MixAppendToPK(DuckDB *db, atomic<idx_t> *count) {

	Connection con(*db);
	for (idx_t i = 0; i < 100; i++) {

		auto result = con.Query("INSERT INTO integers VALUES ($1)", i);
		if (!result->HasError()) {
			(*count)++;
			continue;
		}

		CheckConstraintViolation(result->ToString());
	}
}

static void MixUpdatePK(DuckDB *db, idx_t thread_idx) {

	std::uniform_int_distribution<> distribution(1, 100);
	std::mt19937 gen;
	gen.seed(thread_idx);

	Connection con(*db);
	for (idx_t i = 0; i < 100; i++) {

		idx_t old_value = distribution(gen);
		idx_t new_value = 100 + distribution(gen);

		auto result =
		    con.Query("UPDATE integers SET i =" + to_string(new_value) + " WHERE i = " + to_string(old_value));
		if (result->HasError()) {
			CheckConstraintViolation(result->ToString());
		}
	}
}

TEST_CASE("Mix updates and inserts on PRIMARY KEY", "[index][.]") {

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));

	atomic<idx_t> atomic_count;
	atomic_count = 0;

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// launch a mix of updating and appending threads
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		if (i % 2) {
			threads[i] = thread(MixUpdatePK, &db, i);
			continue;
		}
		threads[i] = thread(MixAppendToPK, &db, &atomic_count);
	}

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// test the result
	auto result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(atomic_count)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(atomic_count)}));
}

static void TransactionalAppendToPK(DuckDB *db, idx_t thread_idx) {
	duckdb::unique_ptr<QueryResult> result;

	Connection con(*db);
	result = con.Query("BEGIN TRANSACTION");
	if (result->HasError()) {
		FAIL(result->GetError());
	}

	// get the initial count
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i >= 0");
	if (result->HasError()) {
		FAIL(result->GetError());
	}

	auto chunk = result->Fetch();
	auto initial_count = chunk->GetValue(0, 0).GetValue<int32_t>();

	for (idx_t i = 0; i < 50; i++) {

		result = con.Query("INSERT INTO integers VALUES ($1)", (int32_t)(thread_idx * 1000 + i));
		if (result->HasError()) {
			FAIL(result->GetError());
		}

		// check the count
		result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers WHERE i >= 0");
		if (!CHECK_COLUMN(result, 0, {Value::INTEGER(initial_count + i + 1)})) {
			FAIL("Incorrect result in TransactionalAppendToPK");
		}
	}

	result = con.Query("COMMIT");
	if (result->HasError()) {
		FAIL(result->GetError());
	}
}

TEST_CASE("Parallel transactional appends to indexed table", "[index][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// launch many concurrently inserting threads
	thread threads[CONCURRENT_INDEX_THREAD_COUNT];
	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(TransactionalAppendToPK, &db, i);
	}

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}

	// test that the counts are correct
	auto result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(idx_t(CONCURRENT_INDEX_THREAD_COUNT * 50))}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(idx_t(CONCURRENT_INDEX_THREAD_COUNT * 50))}));
}

static void JoinIntegers(Connection *con) {
	for (idx_t i = 0; i < 10; i++) {
		auto result = con->Query("SELECT count(*) FROM integers INNER JOIN integers_2 ON (integers.i = integers_2.i)");
		if (result->HasError()) {
			FAIL();
		}
		if (!CHECK_COLUMN(result, 0, {Value::BIGINT(500000)})) {
			FAIL();
		}
	}

	auto result = con->Query("COMMIT");
	if (result->HasError()) {
		FAIL();
	}
}

TEST_CASE("Concurrent appends during joins", "[index][.]") {

	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));

	// create join tables to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers AS SELECT range AS i FROM range(1000000)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers_2 AS SELECT range AS i FROM range(500000)"));

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	// we need to guarantee that this thread starts before the other threads
	Connection join_con_1(db);
	REQUIRE_NO_FAIL(join_con_1.Query("BEGIN TRANSACTION"));

	Connection join_con_2(db);
	REQUIRE_NO_FAIL(con.Query("SET immediate_transaction_mode=true"));
	REQUIRE_NO_FAIL(join_con_2.Query("BEGIN TRANSACTION"));

	thread threads[CONCURRENT_INDEX_THREAD_COUNT];

	// join the data in join_con_1, which is an uncommitted transaction started
	// before appending any data
	threads[0] = thread(JoinIntegers, &join_con_1);

	atomic<bool> success(true);
	// launch many concurrently writing threads
	for (idx_t i = 2; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i] = thread(AppendToIntegers, &db, &success);
	}

	// join the data in join_con_2, which is an uncommitted transaction started
	// before appending any data
	threads[1] = thread(JoinIntegers, &join_con_2);

	for (idx_t i = 0; i < CONCURRENT_INDEX_THREAD_COUNT; i++) {
		threads[i].join();
	}
	REQUIRE(success);
}
