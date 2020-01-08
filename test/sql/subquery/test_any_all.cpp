#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar ANY/ALL queries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// scalar ANY
	result = con.Query("SELECT 1 = ANY(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 1 = ANY(SELECT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT 1 = ANY(SELECT 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT NULL = ANY(SELECT 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// scalar ALL
	result = con.Query("SELECT 1 = ALL(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 1 = ALL(SELECT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT 1 = ALL(SELECT 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT NULL = ALL(SELECT 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}

TEST_CASE("Test ANY/ALL queries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// ANY is like EXISTS without NULL values
	result = con.Query("SELECT 2 > ANY(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 1 > ANY(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT 4 > ALL(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 1 > ALL(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// NULL input always results in NULL output
	result = con.Query("SELECT NULL > ANY(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL > ALL(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// now with a NULL value in the input
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL)"));

	// ANY returns either true or NULL
	result = con.Query("SELECT 2 > ANY(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 1 > ANY(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// ALL returns either NULL or false
	result = con.Query("SELECT 4 > ALL(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT 1 > ALL(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// NULL input always results in NULL
	result = con.Query("SELECT NULL > ANY(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL > ALL(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}
