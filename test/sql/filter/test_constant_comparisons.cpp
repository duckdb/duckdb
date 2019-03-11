#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test expressions with constant comparisons", "[filter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2, 12)"));

	// Test constant comparisons
	// equality
	result = con.Query("SELECT * FROM integers WHERE 2=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2=3");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// inequality
	result = con.Query("SELECT * FROM integers WHERE 2<>3");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2<>2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// greater than
	result = con.Query("SELECT * FROM integers WHERE 2>1");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2>2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// greater than equals
	result = con.Query("SELECT * FROM integers WHERE 2>=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2>=3");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// less than
	result = con.Query("SELECT * FROM integers WHERE 2<3");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2<2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// less than equals
	result = con.Query("SELECT * FROM integers WHERE 2<=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2<=1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// Test comparisons with NULL
	result = con.Query("SELECT a=NULL FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL=a FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
}

TEST_CASE("More complex constant expressions", "[filter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2, 12)"));

	// Test constant comparisons
	// IN expressions
	result = con.Query("SELECT * FROM integers WHERE 2 IN (2, 3, 4, 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2 NOT IN (2, 3, 4, 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE 2 IN (((1*2)+(1*0))*1, 3, 4, 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE 2 IN ((1+1)*2, 3, 4, 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// CASE expressions
	result = con.Query("SELECT CASE WHEN 1 THEN 13 ELSE 12 END;");
	REQUIRE(CHECK_COLUMN(result, 0, {13}));
	result = con.Query("SELECT * FROM integers WHERE CASE WHEN 2=2 THEN true ELSE false END;");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE CASE WHEN 2=3 THEN true ELSE false END;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}
