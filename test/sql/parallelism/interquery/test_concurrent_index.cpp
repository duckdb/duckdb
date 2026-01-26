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
	auto constraint_violation =
	    result_str.find("violat") != string::npos || result_str.find("Conflict on tuple deletion") != string::npos;
	if (!constraint_violation) {
		FAIL(result_str);
	}
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
