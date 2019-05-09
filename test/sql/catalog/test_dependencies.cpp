#include "catch.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Schema dependencies", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// single schema and dependencies
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE s1.integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	// can't drop: dependency
	REQUIRE_FAIL(con.Query("DROP SCHEMA s1"));
	REQUIRE_NO_FAIL(con.Query("SELECT * FROM s1.integers"));
	// we can drop with cascade though
	REQUIRE_NO_FAIL(con.Query("DROP SCHEMA s1 CASCADE"));
	// this also drops the table
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
	// try again, but this time we commit
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
}

TEST_CASE("Prepared statement dependencies dependencies", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

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
}

TEST_CASE("Default values and dependencies", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// dependency on a sequence in a default value
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER DEFAULT nextval('seq'), j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers (j) VALUES (1), (1), (1), (1), (1)"));

	result = con2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {15}));
	// we can't drop the sequence: the table depends on it
	REQUIRE_FAIL(con.Query("DROP SEQUENCE seq"));
	// cascade drop works
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq CASCADE"));
	// but it also drops the table
	REQUIRE_FAIL(con.Query("SELECT * FROM integers"));

	// dependency on multiple sequences in default value
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq1"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq2"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE integers(i INTEGER DEFAULT nextval('seq' || CAST(nextval('seq') AS VARCHAR)), j INTEGER)"));

	// seq1 exists, so the result of the first default value is 1
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers (j) VALUES (1)"));
	// we can drop seq1 and seq2: the dependency is not fixed
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq1"));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq2"));
	// seq2 does not exist after this drop, so another insert fails
	REQUIRE_FAIL(con.Query("INSERT INTO integers (j) VALUES (1)"));
	// table is now [1, 1]: query it
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// we can't drop seq however: the dependency is fixed
	REQUIRE_FAIL(con.Query("DROP SEQUENCE seq"));
	// need to do a cascading drop
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq CASCADE"));
	// now the table is gone
	REQUIRE_FAIL(con.Query("SELECT * FROM integers"));
}

TEST_CASE("Prepare dependencies and transactions", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// case one: prepared statement is created outside of transaction and committed
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT SUM(i) FROM integers"));

	// begin a transaction in con2
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	// now drop the table in con, with a cascading drop
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers CASCADE"));
	// we can still execute v in con2
	result = con2.Query("EXECUTE v");
	REQUIRE(CHECK_COLUMN(result, 0, {15}));
	// if we try to drop integers we get a conflict though
	REQUIRE_FAIL(con2.Query("DROP TABLE integers CASCADE"));
	// now we rollback
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
	// now we can't use the prepared statement anymore
	REQUIRE_FAIL(con2.Query("EXECUTE v"));

	// case two: prepared statement is created inside transaction
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)"));

	// begin a transaction and create a prepared statement
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT SUM(i) FROM integers"));

	// integers has a dependency: we can't drop it
	REQUIRE_FAIL(con.Query("DROP TABLE integers"));
	// now we can't drop integers even with cascade, because the dependency is not yet committed, this creates a write
	// conflict on attempting to drop the dependency
	REQUIRE_FAIL(con.Query("DROP TABLE integers CASCADE"));

	// use the prepared statement
	result = con2.Query("EXECUTE v");
	REQUIRE(CHECK_COLUMN(result, 0, {15}));
	// and commit
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// now we can commit the table
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers CASCADE"));

	// case three: prepared statement is created inside transaction, and then rolled back
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)"));

	// begin a transaction and create a prepared statement
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT SUM(i) FROM integers"));
	// integers has a dependency: we can't drop it
	REQUIRE_FAIL(con.Query("DROP TABLE integers"));
	// rollback the prepared statement
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
	// depedency was rolled back: now we can drop it
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));

	// case four: deallocate happens inside transaction
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE v AS SELECT SUM(i) FROM integers"));

	// deallocate v inside transaction
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("DEALLOCATE v"));

	// we still can't drop integers because the dependency is still there
	REQUIRE_FAIL(con.Query("DROP TABLE integers"));
	// cascade gives a concurrency conflict
	REQUIRE_FAIL(con.Query("DROP TABLE integers CASCADE"));
	// now rollback the deallocation
	REQUIRE_NO_FAIL(con2.Query("ROLLBACK"));
	// still can't drop the table
	REQUIRE_FAIL(con.Query("DROP TABLE integers"));
	// we can use the prepared statement again
	result = con2.Query("EXECUTE v");
	REQUIRE(CHECK_COLUMN(result, 0, {15}));
	// now do the same as before, but commit the transaction this time
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("DEALLOCATE v"));
	// can't drop yet: not yet committed
	REQUIRE_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_FAIL(con.Query("DROP TABLE integers CASCADE"));
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));
	// after committing we can drop
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("Test prepare dependencies with multiple connections", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	auto con = make_unique<Connection>(db);
	auto con2 = make_unique<Connection>(db);
	auto con3 = make_unique<Connection>(db);

	// simple prepare: begin transaction before the second client calls PREPARE
	REQUIRE_NO_FAIL(con->Query("CREATE TABLE integers(i INTEGER)"));
	// open a transaction in con2, this forces the prepared statement to be kept around until this transaction is closed
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	// we prepare a statement in con
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	// now we drop con while the second client still has an active transaction
	con.reset();
	// now commit the transaction in the second client
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));

	con = make_unique<Connection>(db);
	// three transactions
	// open a transaction in con2, this forces the prepared statement to be kept around until this transaction is closed
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	// create a prepare, this creates a dependency from s1 -> integers
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	// drop the client
	con.reset();
	// now begin a transaction in con3
	REQUIRE_NO_FAIL(con3->Query("BEGIN TRANSACTION"));
	// drop the table integers with cascade, this should drop s1 as well
	REQUIRE_NO_FAIL(con3->Query("DROP TABLE integers CASCADE"));
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));
	REQUIRE_NO_FAIL(con3->Query("COMMIT"));
}

#define REPETITIONS 100
#define SEQTHREADS 10
volatile bool finished = false;

static void RunQueryUntilSuccess(Connection &con, string query) {
	while (true) {
		auto result = con.Query(query);
		if (result->success) {
			break;
		}
	}
}

static void create_drop_table(DuckDB *db) {
	Connection con(*db);

	while (!finished) {
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
		RunQueryUntilSuccess(con, "DROP TABLE integers CASCADE");
	}
}

static void create_use_prepared_statement(DuckDB *db) {
	Connection con(*db);
	unique_ptr<QueryResult> result;

	for (int i = 0; i < REPETITIONS; i++) {
		// printf("[PREPARE] Prepare statement\n");
		RunQueryUntilSuccess(con, "PREPARE s1 AS SELECT SUM(i) FROM integers");
		// printf("[PREPARE] Query prepare\n");
		while (true) {
			// execute the prepared statement until the prepared statement is dropped because of the CASCADE in another
			// thread
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
	for (int i = 0; i < SEQTHREADS; i++) {
		seq_threads[i] = thread(create_use_prepared_statement, &db);
	}
	for (int i = 0; i < SEQTHREADS; i++) {
		seq_threads[i].join();
	}
	finished = true;
	table_thread.join();
}

static void create_drop_schema(DuckDB *db) {
	Connection con(*db);

	while (!finished) {
		// create the schema: this should never fail
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s1"));
		// now wait a bit
		this_thread::sleep_for(chrono::milliseconds(20));
		// perform a cascade drop of the schema
		// this can fail if a thread is still busy creating something inside the schema
		RunQueryUntilSuccess(con, "DROP SCHEMA s1 CASCADE");
	}
}

static void create_use_table_view(DuckDB *db, int threadnr) {
	Connection con(*db);
	unique_ptr<QueryResult> result;
	string tname = "integers" + to_string(threadnr);
	string vname = "v" + to_string(threadnr);

	for (int i = 0; i < REPETITIONS; i++) {
		RunQueryUntilSuccess(con, "CREATE TABLE s1." + tname + "(i INTEGER)");
		con.Query("INSERT INTO s1." + tname + " VALUES (1), (2), (3), (4), (5)");
		RunQueryUntilSuccess(con, "CREATE VIEW s1." + vname + " AS SELECT 42");
		while (true) {
			result = con.Query("SELECT SUM(i) FROM s1." + tname);
			if (!result->success) {
				break;
			} else {
				REQUIRE(CHECK_COLUMN(result, 0, {15}));
			}
			result = con.Query("SELECT * FROM s1." + vname);
			if (!result->success) {
				break;
			} else {
				REQUIRE(CHECK_COLUMN(result, 0, {42}));
			}
		}
	}
}
TEST_CASE("Test parallel dependencies with schemas and tables", "[catalog][.]") {
	DuckDB db(nullptr);
	// FIXME: this test crashes
	return;

	// in this test we create and drop a schema in one thread (with CASCADE drop)
	// in other threads, we create tables and views and query those tables and views

	thread table_thread = thread(create_drop_schema, &db);
	thread seq_threads[SEQTHREADS];
	for (int i = 0; i < SEQTHREADS; i++) {
		seq_threads[i] = thread(create_use_table_view, &db, i);
	}
	for (int i = 0; i < SEQTHREADS; i++) {
		seq_threads[i].join();
	}
	finished = true;
	table_thread.join();
}
