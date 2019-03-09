#include "catch.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test using connection after database is gone", "[api]") {
	auto db = make_unique<DuckDB>(nullptr);
	auto conn = make_unique<Connection>(*db);
	// check that the connection works
	auto result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// destroy the database
	db.reset();
	// try to use the connection
	REQUIRE_FAIL(conn->Query("SELECT 42"));
}

static void long_running_query(Connection *conn) {
	auto result = conn->Query("SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, "
	                          "integers i6, integers i7, integers i8, integers i9, integers i10");
	// the query should fail
	REQUIRE_FAIL(result);
}

TEST_CASE("Test closing database during long running query", "[api]") {
	auto db = make_unique<DuckDB>(nullptr);
	auto conn = make_unique<Connection>(*db);
	// create the database
	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));
	// perform a long running query in the background (many cross products)
	auto background_thread = thread(long_running_query, conn.get());
	// destroy the database
	db.reset();
	// wait for the thread
	background_thread.join();
	// try to use the connection
	REQUIRE_FAIL(conn->Query("SELECT 42"));
}

TEST_CASE("Test closing result after database is gone", "[api]") {
	auto db = make_unique<DuckDB>(nullptr);
	auto conn = make_unique<Connection>(*db);
	// check that the connection works
	auto result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// destroy the database
	db.reset();
	conn.reset();
	result.reset();

	// now the streaming result
	db = make_unique<DuckDB>(nullptr);
	conn = make_unique<Connection>(*db);
	// check that the connection works
	auto streaming_result = conn->SendQuery("SELECT 42");
	// destroy the database
	db.reset();
	conn.reset();
	streaming_result.reset();
}

static void parallel_query(Connection *conn, size_t threadnr) {
	REQUIRE(conn);
	for (size_t i = 0; i < 100; i++) {
		auto result = conn->Query("SELECT * FROM integers ORDER BY i");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
	}
}

TEST_CASE("Test parallel usage of client", "[api]") {
	auto db = make_unique<DuckDB>(nullptr);
	auto conn = make_unique<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	thread threads[20];
	for (size_t i = 0; i < 20; i++) {
		threads[i] = thread(parallel_query, conn.get(), i);
	}

	for (size_t i = 0; i < 20; i++) {
		threads[i].join();
	}
}

TEST_CASE("Test multiple result sets", "[api]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT 42; SELECT 84");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	result = move(result->next);
	REQUIRE(CHECK_COLUMN(result, 0, {84}));
	REQUIRE(!result->next);
}

TEST_CASE("Test fetch API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	unique_ptr<QueryResult> result;

	result = con.SendQuery("CREATE TABLE test (a INTEGER);");

	result = con.Query("select a from test where 1 <> 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.SendQuery("INSERT INTO test VALUES (42)");
	result = con.SendQuery("SELECT a from test");

	REQUIRE(result->Fetch()->GetVector(0).GetValue(0) == Value::INTEGER(42));

	auto materialized_result = con.Query("select a from test");
	REQUIRE(materialized_result->GetValue(0, 0) == Value::INTEGER(42));
}
