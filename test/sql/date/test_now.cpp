#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("NOW function", "[timestamp_func]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// get the millenium of the current date
	// FIXME: this needs to be updated in 982 years
	result = con.Query("SELECT EXTRACT(MILLENNIUM FROM NOW())");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(3)}));

	// the NOW function should return the start time of the transaction
	// hence during a transaction it should not change
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	result = con.Query("SELECT NOW()");
	auto result2 = con.Query("SELECT NOW()");
	REQUIRE(CHECK_COLUMN(result, 0, {result2->collection.GetValue(0, 0)}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}
