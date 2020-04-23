#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test basic TIME functionality", "[time]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create and insert into table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE times(i TIME)"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO times VALUES ('00:01:20'), ('20:08:10.998'), ('20:08:10.33'), ('20:08:10.001'), (NULL)"));

	// check that we can select times
	result = con.Query("SELECT * FROM times");
	REQUIRE(result->sql_types[0] == SQLType::TIME);
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::TIME(0, 1, 20, 0), Value::TIME(20, 8, 10, 998), Value::TIME(20, 8, 10, 330),
	                      Value::TIME(20, 8, 10, 001), Value()}));

	// check that we can convert times to string
	result = con.Query("SELECT cast(i AS VARCHAR) FROM times");
	REQUIRE(CHECK_COLUMN(
	    result, 0, {Value("00:01:20"), Value("20:08:10.998"), Value("20:08:10.330"), Value("20:08:10.001"), Value()}));
}
