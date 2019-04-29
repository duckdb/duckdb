#include "catch.hpp"
#include "test_helpers.hpp"

#include <thread>
#include <chrono>

using namespace duckdb;
using namespace std;

TEST_CASE("Test dependencies with multiple connections", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// single schema and dependencies
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE s1.integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	REQUIRE_FAIL(con.Query("DROP SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA s1 CASCADE"));
	REQUIRE_FAIL(con.Query("SELECT * FROM s1.integers"));

	// schemas and dependencies
	// create a schema and a table inside the schema
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE s1.integers(i INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	// drop the table in con1
	REQUIRE_NO_FAIL(con.Query("DROP TABLE s1.integers"));
	// we can't drop the schema from con2 because the table still exists for con2!
	REQUIRE_FAIL(con2.Query("DROP SCHEMA s1"));
	// now rollback the table drop
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
	// the table exists again
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	// try again
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	// drop the schema entirely now
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA s1 CASCADE"));
	// we can still query the table from con2
	REQUIRE_NO_FAIL(con2.Query("SELECT * FROM s1.integers"));
	// even after we commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con2.Query("SELECT * FROM s1.integers"));
	// however if we end the transaction in con2 the schema is gone
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
	REQUIRE_FAIL(con2.Query("CREATE TABLE s1.dummy(i INTEGER)"));

	// prepared statements and dependencies
	// dependency on a bound table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT * FROM integers"));
	REQUIRE_NO_FAIL(con2.Query("EXECUTE v"));
	// cannot drop table now
	REQUIRE_FAIL(con.Query("DROP TABLE integers"));
	// but CASCADE drop should work
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers CASCADE"));
	// after the cascade drop the prepared statement is invalidated
	REQUIRE_FAIL(con2.Query("EXECUTE v"));

	// dependency on a sequence for prepare
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT nextval('seq')"));
	result = con2.Query("EXECUTE v");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// cannot drop sequence now
	REQUIRE_FAIL(con.Query("DROP SEQUENCE seq"));
	// check that the prepared statement still works
	result = con2.Query("EXECUTE v");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// cascade drop
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq CASCADE"));
	// after the cascade drop the prepared statement is invalidated
	REQUIRE_FAIL(con2.Query("EXECUTE v"));

	// dependency on a sequence in a default value
}

TEST_CASE("Test prepare dependencies with multiple connections", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	auto con = make_unique<Connection>(db);
	auto con2 = make_unique<Connection>(db);
	auto con3 = make_unique<Connection>(db);

	// simple prepare: begin transaction before the second client calls PREPARE
	REQUIRE_NO_FAIL(con->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	con.reset();
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));

	con = make_unique<Connection>(db);
	// three transactions
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	con.reset();
	REQUIRE_NO_FAIL(con3->Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con3->Query("DROP TABLE integers CASCADE"));
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));
	REQUIRE_NO_FAIL(con3->Query("COMMIT"));
}

#define REPETITIONS 100
#define SEQTHREADS 10
volatile bool finished = false;

static void create_drop_table(DuckDB *db) {
	Connection con(*db);

	while(!finished) {
		// printf("[TABLE] Create table\n");
		// create the table: this should never fail
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
		// now wait a bit
		this_thread::sleep_for(chrono::milliseconds(20));
		// printf("[TABLE] Drop table\n");
		// perform a cascade drop of the table
		// this can fail if a thread is still busy preparing a statement
		while(true) {
			auto result = con.Query("DROP TABLE integers CASCADE");
			if (result->success) {
				break;
			}
		}
	}
}

static void create_use_prepared_statement(DuckDB *db) {
	Connection con(*db);
	unique_ptr<QueryResult> result;

	for(int i = 0; i < REPETITIONS; i++) {
		// printf("[PREPARE] Prepare statement\n");
		while(true) {
			// create the prepared statement
			result = con.Query("PREPARE s1 AS SELECT SUM(i) FROM integers");
			if (result->success) {
				break;
			}
		}
		// printf("[PREPARE] Query prepare\n");
		while(true) {
			// execute the prepared statement until the statement is dropped
			result = con.Query("EXECUTE s1");
			if (!result->success) {
				break;
			} else {
				REQUIRE(CHECK_COLUMN(result, 0, {15}));
			}
		}
	}
}

TEST_CASE("Test parallel dependencies in multiple connections", "[catalog][.]") {
	DuckDB db(nullptr);

	// in this test we create and drop a table in one thread (with CASCADE drop)
	// in the other thread, we create a prepared statement and execute it
	// the prepared statement depends on the table
	// hence when the CASCADE drop is executed the prepared statement also needs to be dropped

	thread table_thread = thread(create_drop_table, &db);
	thread seq_threads[SEQTHREADS];
	for(int i = 0; i < SEQTHREADS; i++) {
	        seq_threads[i] = thread(create_use_prepared_statement, &db);
	}
	for(int i = 0; i < SEQTHREADS; i++) {
	        seq_threads[i].join();
	}
	finished = true;
	table_thread.join();
}
