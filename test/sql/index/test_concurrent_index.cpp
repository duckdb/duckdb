#include "catch.hpp"

#include <thread>
#include <vector>

// needs to happen after the <thread> include since we redefine make_unique
#include "main/appender.hpp"
#include "test_helpers.hpp"





using namespace duckdb;
using namespace std;

atomic<bool> is_finished;

#define THREAD_COUNT 20
#define INSERT_COUNT 2000

static void read_from_integers(DuckDB *db, size_t threadnr) {
	REQUIRE(db);
	DuckDBConnection con(*db);

	while (!is_finished) {
		auto result = con.Query("SELECT i FROM integers WHERE i = " + to_string(threadnr * 10000));
		REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(threadnr * 10000)}));
	}
}

TEST_CASE("Concurrent reads during index creation", "[index][.]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// append a bunch of entries
	Appender appender(db, DEFAULT_SCHEMA, "integers");
	for (size_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();

	is_finished = false;
	// now launch a bunch of reading threads
	thread threads[THREAD_COUNT];
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(read_from_integers, &db, i);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	is_finished = true;

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that we can probe the index correctly
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=500000");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

static void append_to_integers(DuckDB *db, size_t threadnr) {
	REQUIRE(db);
	DuckDBConnection con(*db);

	Appender appender(*db, DEFAULT_SCHEMA, "integers");
	for (size_t i = 0; i < INSERT_COUNT; i++) {
		appender.BeginRow();
		appender.AppendInteger(1);
		appender.EndRow();
	}
	appender.Commit();
}

TEST_CASE("Concurrent writes during index creation", "[index][.]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create a single table to append to
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// append a bunch of entries
	Appender appender(db, DEFAULT_SCHEMA, "integers");
	for (size_t i = 0; i < 1000000; i++) {
		appender.BeginRow();
		appender.AppendInteger(i);
		appender.EndRow();
	}
	appender.Commit();

	// now launch a bunch of concurrently writing threads (!)
	thread threads[THREAD_COUNT];
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(append_to_integers, &db, i);
	}

	// create the index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// now test that we can probe the index correctly
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1 + THREAD_COUNT * INSERT_COUNT}));
}
