#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Simple table creation transaction tests", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	// create two connections
	Connection con_one(db);
	Connection con_two(db);

	// start transactions
	result = con_one.Query("BEGIN TRANSACTION");
	REQUIRE(result->success);
	result = con_two.Query("BEGIN TRANSACTION");
	REQUIRE(result->success);

	// create a table on connection one
	result = con_one.Query("CREATE TABLE integers(i INTEGER)");
	REQUIRE(result->success);
	// connection one should be able to query the table
	result = con_one.Query("SELECT * FROM integers");
	REQUIRE(result->success);
	// connection two should not be able to
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(!result->success);
	// if we rollback, nobody should be able to query the table
	result = con_one.Query("ROLLBACK");
	REQUIRE(result->success);

	result = con_one.Query("SELECT * FROM integers");
	REQUIRE(!result->success);
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(!result->success);

	// now if we commit the table
	result = con_one.Query("BEGIN TRANSACTION");
	REQUIRE(result->success);
	result = con_one.Query("CREATE TABLE integers(i INTEGER)");
	REQUIRE(result->success);
	result = con_one.Query("COMMIT");
	REQUIRE(result->success);

	// con two STILL should not see it because it was started before the
	// transaction committed
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(!result->success);

	// but if we rollback and start a new transaction it should see it
	result = con_two.Query("ROLLBACK");
	REQUIRE(result->success);
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(result->success);

	// serialize conflict

	// start transactions
	result = con_one.Query("BEGIN TRANSACTION");
	REQUIRE(result->success);
	result = con_two.Query("BEGIN TRANSACTION");
	REQUIRE(result->success);

	// create a table on connection one
	result = con_one.Query("CREATE TABLE integers2(i INTEGER)");
	REQUIRE(result->success);

	// create a table on connection two with the same name
	result = con_one.Query("CREATE TABLE integers2(i INTEGER)");
	REQUIRE(!result->success);
}

TEST_CASE("Stacked schema changes", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	// create two connections
	Connection con(db);

	con.Query("CREATE TABLE a(i INTEGER)");
	con.Query("INSERT INTO a VALUES (44)");
	result = con.Query("SELECT i FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {44}));

	con.Query("BEGIN TRANSACTION");
	con.Query("DROP TABLE a");
	con.Query("CREATE TABLE a(i INTEGER)");
	con.Query("INSERT INTO a VALUES (45)");
	result = con.Query("SELECT i FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {45}));
	con.Query("ROLLBACK");

	result = con.Query("SELECT i FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {44}));

	con.Query("BEGIN TRANSACTION");
	con.Query("DROP TABLE a");
	con.Query("CREATE TABLE a(i INTEGER)");
	con.Query("INSERT INTO a VALUES (46)");
	result = con.Query("SELECT i FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {46}));
	result = con.Query("COMMIT");

	result = con.Query("SELECT i FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {46}));
}
