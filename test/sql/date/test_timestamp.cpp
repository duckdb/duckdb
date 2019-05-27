#include "catch.hpp"
#include "common/types/timestamp.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TIMESTAMP type", "[timestamp]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// creates a timestamp table with a timestamp column and inserts a value
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE IF NOT EXISTS timestamp (t TIMESTAMP);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamp VALUES ('2008-01-01 00:00:01'), (NULL)"));

	// check if we can select timestamps
	result = con.Query("SELECT timestamp '2017-07-23 13:10:11';");
	REQUIRE(result->sql_types[0] == SQLType(SQLTypeId::TIMESTAMP));
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(Timestamp::FromString("2017-07-23 13:10:11")), Value()}));

	result = con.Query("SELECT t1 FROM test_timestamp;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("2008-01-01 00:00:01"), Value()}));
}
