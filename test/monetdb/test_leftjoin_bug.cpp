#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("MonetDB Test: leftjoin.Bug-3981.sql", "[monetdb]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query(
	    "SELECT * FROM ( SELECT 'apple' as fruit UNION ALL SELECT 'banana' ) a JOIN ( SELECT 'apple' as fruit UNION "
	    "ALL SELECT 'banana' ) b ON a.fruit=b.fruit LEFT JOIN ( SELECT 1 as isyellow ) c ON b.fruit='banana' ORDER BY "
	    "1, 2, 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {"apple", "banana"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"apple", "banana"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 1}));
}

TEST_CASE("MonetDB Test: null_matches_in_outer.Bug-6398.sql", "[monetdb]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table \"E\" (\"intCol\" bigint, \"stringCol\" varchar);"));

	REQUIRE_NO_FAIL(con.Query("insert into \"E\" values (0, 'zero');"));
	REQUIRE_NO_FAIL(con.Query("insert into \"E\" values (1, 'one');"));
	REQUIRE_NO_FAIL(con.Query("insert into \"E\" values (2, 'two');"));
	REQUIRE_NO_FAIL(con.Query("insert into \"E\" values (null, null);"));

	REQUIRE_NO_FAIL(con.Query("create table \"I\" (\"intCol\" bigint, \"stringCol\" varchar);"));

	REQUIRE_NO_FAIL(con.Query("insert into \"I\" values (2, 'due');"));
	REQUIRE_NO_FAIL(con.Query("insert into \"I\" values (4, 'quattro');"));
	REQUIRE_NO_FAIL(con.Query("insert into \"I\" values (null, 'this is not null')"));

	result = con.Query("select * from \"E\" left outer join \"I\" on \"E\".\"intCol\" = \"I\".\"intCol\" or "
	                   "(\"E\".\"intCol\" is null and  \"I\".\"intCol\" is null) ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), "zero", "one", "two"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value(), Value(), 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {"this is not null", Value(), Value(), "due"}));
}

TEST_CASE("MonetDB Test: outerjoin_project.Bug-3725.sql", "[monetdb]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table a (a integer);"));
	REQUIRE_NO_FAIL(con.Query("create table b (a integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into a values (1);"));

	result = con.Query("select * from a left join (select a, 20 from b) as x using (a);");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
}
