#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test using appender after connection is gone", "[api]") {
	auto db = make_unique<DuckDB>(nullptr);
	auto conn = make_unique<Connection>(*db);
	unique_ptr<Appender> appender;
	unique_ptr<QueryResult> result;
	// create an appender for a non-existing table fails
	REQUIRE_THROWS(make_unique<Appender>(*conn, "integers"));
	// now create the table and create the appender
	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	appender = make_unique<Appender>(*conn, "integers");

	// we can use the appender
	appender->BeginRow();
	appender->Append<int32_t>(1);
	appender->EndRow();

	appender->Flush();

	result = conn->Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// removing the connection invalidates the appender
	conn.reset();
	REQUIRE_THROWS(appender->BeginRow());
	appender.reset();

	// now create the appender and connection again
	conn = make_unique<Connection>(*db);
	appender = make_unique<Appender>(*conn, "integers");

	// removing the database invalidates both the connection and the appender
	db.reset();

	REQUIRE_FAIL(conn->Query("SELECT * FROM integers"));
	REQUIRE_THROWS(appender->BeginRow());
}

TEST_CASE("Test appender and connection destruction order", "[api]") {
	for (idx_t i = 0; i < 6; i++) {
		auto db = make_unique<DuckDB>(nullptr);
		auto con = make_unique<Connection>(*db);
		REQUIRE_NO_FAIL(con->Query("CREATE TABLE integers(i INTEGER)"));
		auto appender = make_unique<Appender>(*con, "integers");

		switch (i) {
		case 0:
			// db - con - appender
			db.reset();
			con.reset();
			appender.reset();
			break;
		case 1:
			// db - appender - con
			db.reset();
			appender.reset();
			con.reset();
			break;
		case 2:
			// con - db - appender
			con.reset();
			db.reset();
			appender.reset();
			break;
		case 3:
			// con - appender - db
			con.reset();
			appender.reset();
			db.reset();
			break;
		case 4:
			// appender - con - db
			appender.reset();
			con.reset();
			db.reset();
			break;
		default:
			// appender - db - con
			appender.reset();
			db.reset();
			con.reset();
			break;
		}
	}
}

TEST_CASE("Test using appender after table is dropped", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// create the table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// now create the appender
	Appender appender(con, "integers");

	// appending works initially
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.EndRow();
	appender.Flush();

	// now drop the table
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	// now appending fails
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.EndRow();
	REQUIRE_THROWS(appender.Flush());
}

TEST_CASE("Test using appender after table is altered", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// create the table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// now create the appender
	Appender appender(con, "integers");

	// appending works initially
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.EndRow();
	appender.Flush();

	// now create a new table with the same name but different types
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i VARCHAR)"));
	// now appending fails
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.EndRow();
	REQUIRE_THROWS(appender.Flush());
}

TEST_CASE("Test appenders and transactions", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	// create the table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	// now create the appender
	Appender appender(con, "integers");

	// rollback an append
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.EndRow();
	appender.Flush();
	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// we can still use the appender in auto commit mode
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.EndRow();
	appender.Flush();

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test using multiple appenders", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	// create the table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2(i VARCHAR, j DATE)"));
	// now create the appender
	Appender a1(con, "t1");
	Appender a2(con, "t2");

	// begin appending from both
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	a1.BeginRow();
	a1.Append<int32_t>(1);
	a1.EndRow();
	a1.Flush();

	a2.BeginRow();
	a2.Append<const char *>("hello");
	a2.Append<Value>(Value::DATE(1992, 1, 1));
	a2.EndRow();
	a2.Flush();

	result = con.Query("SELECT * FROM t1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT * FROM t2");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::DATE(1992, 1, 1)}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	result = con.Query("SELECT * FROM t1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}

TEST_CASE("Test usage of appender interleaved with connection usage", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	// create the table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	Appender appender(con, "t1");

	appender.AppendRow(1);
	appender.Flush();

	result = con.Query("SELECT * FROM t1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	appender.AppendRow(2);
	appender.Flush();

	result = con.Query("SELECT * FROM t1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}
