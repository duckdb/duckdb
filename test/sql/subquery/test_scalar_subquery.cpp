#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar subqueries", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT 1+(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT 1=(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT 1<>(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT 1=(SELECT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL=(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}

TEST_CASE("Test scalar EXISTS query", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT EXISTS(SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	result = con.Query("SELECT EXISTS(SELECT 1) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true, true}));

	result = con.Query("SELECT EXISTS(SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT EXISTS(SELECT * FROM integers WHERE i IS NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
}

TEST_CASE("Test scalar IN query", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("SELECT 1 IN (SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT NULL IN (SELECT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT 1 IN (SELECT NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT 1 IN (SELECT 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	result = con.Query("SELECT 4 IN (SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT 1 IN (SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT 1 IN (SELECT * FROM integers) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true}));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL)"));

	result = con.Query("SELECT 4 IN (SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT 1 IN (SELECT * FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT * FROM integers WHERE (4 IN (SELECT * FROM integers)) IS NULL ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));

	result = con.Query("SELECT * FROM integers WHERE (i IN (SELECT * FROM integers)) IS NULL ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}
