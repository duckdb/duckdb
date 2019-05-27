#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TIMESTAMP type", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// creates a timestamp table with a timestamp type value
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE IF NOT EXISTS timestamp (t TIMESTAMP);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES ('2008-01-01 00:00:01'), (NULL)"));

	result = con.Query("SELECT t1 FROM test_timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("2008-01-01 00:00:01"), Value()}));

	result = con.Query("SELECT TIMESTAMP('2017-07-23',  '13:10:11');");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("2017-07-23 13:10:11")}));
}
