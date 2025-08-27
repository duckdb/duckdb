
#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test upserting through the query appender", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER PRIMARY KEY, value VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (1, 'hello')"));

	duckdb::vector<LogicalType> types;
	types.push_back(LogicalType::INTEGER);
	types.push_back(LogicalType::VARCHAR);

	QueryAppender appender(con, "INSERT OR REPLACE INTO tbl FROM appended_data", types);
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.Append("world");
	appender.EndRow();

	appender.BeginRow();
	appender.Append<int32_t>(2);
	appender.Append("again");
	appender.EndRow();

	// this should succeed
	appender.Flush();

	result = con.Query("SELECT * FROM tbl ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"world", "again"}));
}

TEST_CASE("Test custom column + table names in the query appender", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER PRIMARY KEY, value VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (1, 'hello')"));

	duckdb::vector<LogicalType> types;
	types.push_back(LogicalType::INTEGER);
	types.push_back(LogicalType::VARCHAR);

	duckdb::vector<string> column_names;
	column_names.push_back("i");
	column_names.push_back("value");

	QueryAppender appender(
	    con, "MERGE INTO tbl USING my_appended_data USING (i) WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT",
	    types, column_names, "my_appended_data");
	appender.BeginRow();
	appender.Append<int32_t>(1);
	appender.Append("world");
	appender.EndRow();

	appender.BeginRow();
	appender.Append<int32_t>(2);
	appender.Append("again");
	appender.EndRow();

	// this should succeed
	appender.Flush();

	result = con.Query("SELECT * FROM tbl ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"world", "again"}));
}

TEST_CASE("Test various error conditions of the query appender", "[appender]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER PRIMARY KEY, value VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (1, 'hello')"));

	duckdb::vector<LogicalType> types;
	types.push_back(LogicalType::INTEGER);
	types.push_back(LogicalType::VARCHAR);

	SECTION("Unsupported query type") {
		QueryAppender appender(con, "SELECT * FROM appended_data", types);
		appender.AppendRow(1, "world");
		REQUIRE_THROWS(appender.Flush());
	}
	SECTION("Multiple queries") {
		QueryAppender appender(con, "INSERT INTO tbl FROM appended_data; INSERT INTO tbl FROM appended_data;", types);
		appender.AppendRow(1, "world");
		REQUIRE_THROWS(appender.Flush());
	}
	SECTION("Parser error in query") {
		QueryAppender appender(con, "INSERT INTO tbl FROM", types);
		appender.AppendRow(1, "world");
		REQUIRE_THROWS(appender.Flush());
	}
	SECTION("Run-time error in query") {
		QueryAppender appender(con, "INSERT INTO tbl SELECT col2::INTEGER, col2 FROM appended_data", types);
		appender.AppendRow(1, "world");
		REQUIRE_THROWS(appender.Flush());
	}
}
