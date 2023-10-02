#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test appender with lists", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lists(i INTEGER[])"));

	// append a value to the table
	Appender appender(con, "lists");

	auto list_value = Value::LIST({Value::INTEGER(3)});
	auto empty_list_value = Value::EMPTYLIST(LogicalType::INTEGER);

	appender.AppendRow(list_value);
	appender.AppendRow(empty_list_value);

	appender.Close();

	// we can select the value now
	result = con.Query("SELECT * FROM lists");
	REQUIRE(CHECK_COLUMN(result, 0, {list_value, empty_list_value}));
}

TEST_CASE("Test appender with nested lists", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lists(i INTEGER[][])"));

	// append a value to the table
	Appender appender(con, "lists");

	auto list_value =
	    Value::LIST({Value::LIST({Value::INTEGER(1)}), Value::LIST({Value::INTEGER(2), Value::INTEGER(3)})});
	auto empty_list_value = Value::EMPTYLIST(LogicalType::LIST(LogicalType::INTEGER));
	auto null_list_value = Value(LogicalType::LIST(LogicalType::LIST(LogicalType::INTEGER)));
	appender.AppendRow(list_value);
	appender.AppendRow(empty_list_value);
	appender.AppendRow(null_list_value);

	appender.Close();

	// we can select the value now
	result = con.Query("SELECT * FROM lists");
	REQUIRE(CHECK_COLUMN(result, 0, {list_value, empty_list_value, null_list_value}));
}
