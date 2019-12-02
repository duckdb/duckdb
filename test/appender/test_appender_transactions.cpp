#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test the way appenders interact with transactions", "[appender]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// begin a transaction manually
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	return;
	// append a value to the table
	Appender appender(con, "integers");

	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.EndRow();

	appender.Close();

	// we can select the value now
	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// rolling back cancels the transaction
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}
