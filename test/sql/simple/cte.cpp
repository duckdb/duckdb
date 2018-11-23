
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Common Table Expressions (CTE)", "[cte]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("create table a(i integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into a values (42);"));

	result =
	    con.Query("with cte1 as (Select i as j from a) select * from cte1;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("with cte1 as (Select i as j from a), cte2 as (select "
	                   "ref.j as k from cte1 as ref), cte3 as (select ref2.j+1 "
	                   "as i from cte1 as ref2) select * from cte2 , cte3;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {43}));

	result = con.Query(
	    "with cte1 as (select i as j from a), cte2 as (select ref.j as k from "
	    "cte1 as ref), cte3 as (select ref2.j+1 as i from cte1 as ref2) select "
	    "* from cte2 union all select * FROM cte3;");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));

	// reference to CTE before its actually defined
	result = con.Query(
	    "with cte3 as (select ref2.j as i from cte1 as ref2), cte1 as (Select "
	    "i as j from a), cte2 as (select ref.j+1 as k from cte1 as ref) select "
	    "* from cte2 union all select * FROM cte3;");
	REQUIRE(CHECK_COLUMN(result, 0, {43, 42}));

	// multiple uses of same CTE
	result = con.Query("with cte1 as (Select i as j from a) select * "
	                   "from cte1 cte11, cte1 cte12;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));
}
