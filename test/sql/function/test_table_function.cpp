#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Table functions", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));

	// SELECT * from table function
	result = con.Query("SELECT * FROM pragma_table_info('integers');");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"i", "j"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER", "INTEGER"}));
	REQUIRE(CHECK_COLUMN(result, 3, {false, false}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {false, false}));

	// project single column
	result = con.Query("SELECT name FROM pragma_table_info('integers');");
	REQUIRE(CHECK_COLUMN(result, 0, {"i", "j"}));

	// project column that is not in function return
	REQUIRE_FAIL(con.Query("SELECT blablabla FROM pragma_table_info('integers');"));

	// join with table function
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE join_table(name VARCHAR, value INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO join_table VALUES ('i', 33), ('j', 44)"));

	result = con.Query("SELECT a.name, cid, value FROM pragma_table_info('integers') AS a "
	                   "INNER JOIN join_table ON a.name=join_table.name ORDER BY a.name;");
	REQUIRE(CHECK_COLUMN(result, 0, {"i", "j"}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 2, {33, 44}));

	// table function in subquery
	result = con.Query("SELECT cid, name FROM (SELECT * FROM "
	                   "pragma_table_info('integers')) AS a");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"i", "j"}));
}
