
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Simple table creation transaction tests", "[transactions]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	// create two connections
	DuckDBConnection con_one(db);
	DuckDBConnection con_two(db);

	// start transactions
	result = con_one.Query("BEGIN TRANSACTION");
	REQUIRE(result->GetSuccess());
	result = con_two.Query("BEGIN TRANSACTION");
	REQUIRE(result->GetSuccess());

	// create a table on connection one
	result = con_one.Query("CREATE TABLE integers(i INTEGER)");
	REQUIRE(result->GetSuccess());
	// connection one should be able to query the table
	result = con_one.Query("SELECT * FROM integers");
	REQUIRE(result->GetSuccess());
	// connection two should not be able to
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(!result->GetSuccess());
	// if we rollback, nobody should be able to query the table
	result = con_one.Query("ROLLBACK");
	REQUIRE(result->GetSuccess());

	result = con_one.Query("SELECT * FROM integers");
	REQUIRE(!result->GetSuccess());
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(!result->GetSuccess());

	// now if we commit the table
	result = con_one.Query("BEGIN TRANSACTION");
	REQUIRE(result->GetSuccess());
	result = con_one.Query("CREATE TABLE integers(i INTEGER)");
	REQUIRE(result->GetSuccess());
	result = con_one.Query("COMMIT");
	REQUIRE(result->GetSuccess());

	// con two STILL should not see it because it was started before the
	// transaction committed
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(!result->GetSuccess());

	// but if we rollback and start a new transaction it should see it
	result = con_two.Query("ROLLBACK");
	REQUIRE(result->GetSuccess());
	result = con_two.Query("SELECT * FROM integers");
	REQUIRE(result->GetSuccess());

	// serialize conflict

	// start transactions
	result = con_one.Query("BEGIN TRANSACTION");
	REQUIRE(result->GetSuccess());
	result = con_two.Query("BEGIN TRANSACTION");
	REQUIRE(result->GetSuccess());

	// create a table on connection one
	result = con_one.Query("CREATE TABLE integers2(i INTEGER)");
	REQUIRE(result->GetSuccess());

	// create a table on connection two with the same name
	result = con_one.Query("CREATE TABLE integers2(i INTEGER)");
	REQUIRE(!result->GetSuccess());
}
