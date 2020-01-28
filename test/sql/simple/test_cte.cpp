#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Common Table Expressions (CTE)", "[cte]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table a(i integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into a values (42);"));

	result = con.Query("with cte1 as (Select i as j from a) select * from cte1;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("with cte1 as (Select i as j from a), cte2 as (select "
	                   "ref.j as k from cte1 as ref), cte3 as (select ref2.j+1 "
	                   "as i from cte1 as ref2) select * from cte2 , cte3;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {43}));

	result = con.Query("with cte1 as (select i as j from a), cte2 as (select ref.j as k from "
	                   "cte1 as ref), cte3 as (select ref2.j+1 as i from cte1 as ref2) select "
	                   "* from cte2 union all select * FROM cte3;");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));

	// reference to CTE before its actually defined
	result = con.Query("with cte3 as (select ref2.j as i from cte1 as ref2), cte1 as (Select "
	                   "i as j from a), cte2 as (select ref.j+1 as k from cte1 as ref) select "
	                   "* from cte2 union all select * FROM cte3;");
	REQUIRE(CHECK_COLUMN(result, 0, {43, 42}));

	// multiple uses of same CTE
	result = con.Query("with cte1 as (Select i as j from a) select * "
	                   "from cte1 cte11, cte1 cte12;");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));

	// refer to CTE in subquery
	result = con.Query("with cte1 as (Select i as j from a) select * from cte1 "
	                   "where j = (select max(j) from cte1 as cte2);");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Test Recursive Common Table Expressions (CTE)", "[rec_cte]") {
    unique_ptr<QueryResult> result;
    DuckDB db(nullptr);
    Connection con(db);
    con.EnableQueryVerification();

    // simple recursive CTE
    result = con.Query("with recursive t as (select 1 as x union all select x+1 from t where x < 3) select * from t");
    REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

    // simple recursive CTE with an alias
    result = con.Query("with recursive t as (select 1 as x union all select x+1 from t as m where m.x < 3) select * from t");
    REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

    // recursive CTE with multiple references and aliases
    result = con.Query("with recursive t as (select 1 as x union all select m.x+f.x from t as m, t as f where m.x < 3) select * from t");
    REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 4}));


    // aggregate functions are not allowed in the recursive term of ctes
    REQUIRE_FAIL(con.Query("with recursive t as (select 1 as x union all select sum(x+1) from t where x < 3) select * from t"));
}