#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Abort appender due to primary key conflict", "[appender]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));

	auto appender = con.OpenAppender(DEFAULT_SCHEMA, "integers");
	appender->BeginRow();
	appender->AppendInteger(1);
	appender->EndRow();
	// FIXME: this should fail
	appender->Flush();
	con.CloseAppender();
}
