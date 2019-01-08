#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test expressions with constant comparisons", "[filter]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
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
}
