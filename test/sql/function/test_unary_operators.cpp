#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test unary operators", "[function]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (2)"));

	result = con.Query("SELECT ++-++-+i FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	//! test simple unary operators
	result = con.Query("SELECT +i FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT -i FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {-2}));

	// we can also stack unary functions
	result = con.Query("SELECT +++++++i FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT ++-++-+i FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT -+-+-+-+-i FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {-2}));

	// cannot apply these to a string
	REQUIRE_FAIL(con.Query("SELECT +'hello'"));
	REQUIRE_FAIL(con.Query("SELECT -'hello'"));

	// cannot apply these to a date either
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dates(d DATE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO dates VALUES ('1992-02-02')"));

	REQUIRE_FAIL(con.Query("SELECT +d FROM dates"));
	REQUIRE_FAIL(con.Query("SELECT -d FROM dates"));

	//! postfix operators not implemented
	REQUIRE_FAIL(con.Query("SELECT 2!"));
}
