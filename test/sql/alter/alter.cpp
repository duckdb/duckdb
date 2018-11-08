
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ALTER TABLE", "[alter]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// CREATE TABLE AND ALTER IT TO ADD ONE COLUMN
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE IF NOT EXISTS test(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("ALTER TABLE test ADD COLUMN k INTEGER"));
	
	result = con.Query(
	    "SELECT i, j, k FROM test");
	REQUIRE(result->names.size() == 3);
	REQUIRE(result->names[0] == "i");
	REQUIRE(result->names[0] == "j");
	REQUIRE(result->names[0] == "k");

	//ALTER TABLE TO ADD ONE COLUMN
	REQUIRE_NO_FAIL(
	    con.Query("ALTER TABLE integers ADD COLUMN l INTEGER"));
	result = con.Query(
	    "SELECT i, j, k, l FROM test");
	REQUIRE(result->names.size() == 4);
	REQUIRE(result->names[0] == "i");
	REQUIRE(result->names[0] == "j");
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[0] == "l");

	//ALTER TABLE TO DROP ONE COLUMN
	/*REQUIRE_NO_FAIL(
	    con.Query("ALTER TABLE integers DROP COLUMN l"));
	result = con.Query(
	    "SELECT i, j, k, l FROM test");
	REQUIRE(result->names.size() == 3);
	REQUIRE(result->names[0] == "i");
	REQUIRE(result->names[0] == "j");
	REQUIRE(result->names[0] == "k");
	REQUIRE(result->names[0] == "l");

	// DROP TABLE IF EXISTS
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS test"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE IF EXISTS test"));*/
}
