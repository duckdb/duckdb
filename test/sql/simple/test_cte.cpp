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

	// duplicate CTE alias
	REQUIRE_FAIL(con.Query("with cte1 as (select 42), cte1 as (select 42) select * FROM cte1;"));

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

TEST_CASE("Test Recursive Common Table Expressions UNION ALL (CTE)", "[rec_cte_union_all]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// simple recursive CTE
	result = con.Query("with recursive t as (select 1 as x union all select x+1 from t where x < 3) select * from t");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// simple recursive CTE with an alias
	result =
	    con.Query("with recursive t as (select 1 as x union all select x+1 from t as m where m.x < 3) select * from t");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// recursive CTE with multiple references and aliases
	result = con.Query("with recursive t as (select 1 as x union all select m.x+f.x from t as m, t as f where m.x < 3) "
	                   "select * from t");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 4}));

	// strings and multiple columns
	result = con.Query("with recursive t as (select 1 as x, 'hello' as y union all select x+1, y || '-' || 'hello' "
	                   "from t where x < 3) select * from t;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "hello-hello", "hello-hello-hello"}));

	// referencing same CTE multiple times
	result = con.Query("with recursive t as (select 1 as x union all select x+1 from t where x < 3) select min(a1.x) "
	                   "from t a1, t a2;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// nested uncorrelated subquery
	result = con.Query(
	    "with recursive t as (select 1 as x union all select x+(SELECT 1) from t where x < 3) select * from t;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// nested correlated subquery
	// Not supported at the moment.
	//    REQUIRE_FAIL(con.Query("with recursive t as (select 1 as x union all select (SELECT x+1) from t where x < 3)
	//    select * from t;")); result = con.Query("with recursive t as (select 1 as x union all select (SELECT x+1) from
	//    t where x < 3) select * from t;"); REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// use with recursive in table creation
	REQUIRE_NO_FAIL(con.Query("create table integers as with recursive t as (select 1 as x union all select x+1 from t "
	                          "where x < 3) select * from t;"));

	// more complex uncorrelated subquery
	result = con.Query("with recursive t as (select (select min(x) from integers) as x union all select x+1 from t "
	                   "where x < 3) select * from t;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// aggregate functions are not allowed in the recursive term of ctes
	REQUIRE_FAIL(
	    con.Query("with recursive t as (select 1 as x union all select sum(x+1) from t where x < 3) select * from t"));

	// order by is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query(
	    "with recursive t as (select 1 as x union all select sum(x+1) from t where x < 3 order by x) select * from t"));

	// limit is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query(
	    "with recursive t as (select 1 as x union all select sum(x+1) from t where x < 3 LIMIT 1) select * from t"));

	// offset is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query(
	    "with recursive t as (select 1 as x union all select sum(x+1) from t where x < 3 OFFSET 1) select * from t"));

	// offset is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query("with recursive t as (select 1 as x union all select sum(x+1) from t where x < 3 LIMIT 1 "
	                       "OFFSET 1) select * from t"));
}

TEST_CASE("Test Recursive Common Table Expressions UNION (CTE)", "[rec_cte_union]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// simple recursive CTE
	result =
	    con.Query("with recursive t as (select 1 as x union select x+1 from t where x < 3) select * from t order by x");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// UNION semantics prevents infinite loop here
	result = con.Query("with recursive t as (select 1 as x union select x from t) select * from t");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// simple recursive CTE with an alias
	result = con.Query(
	    "with recursive t as (select 1 as x union select x+1 from t as m where m.x < 3) select * from t order by x");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// recursive CTE with multiple references and aliases
	result = con.Query("with recursive t as (select 1 as x union select m.x+f.x from t as m, t as f where m.x < 3) "
	                   "select * from t order by x");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 4}));

	// strings and multiple columns
	result = con.Query("with recursive t as (select 1 as x, 'hello' as y union select x+1, y || '-' || 'hello' from t "
	                   "where x < 3) select * from t order by x;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "hello-hello", "hello-hello-hello"}));

	// referencing same CTE multiple times
	result = con.Query(
	    "with recursive t as (select 1 as x union select x+1 from t where x < 3) select min(a1.x) from t a1, t a2;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// nested uncorrelated subquery
	result = con.Query(
	    "with recursive t as (select 1 as x union select x+(SELECT 1) from t where x < 3) select * from t order by x;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// nested correlated subquery
	// Not supported at the moment.
	//    REQUIRE_FAIL(con.Query("with recursive t as (select 1 as x union select (SELECT x+1) from t where x < 3)
	//    select * from t;")); result = con.Query("with recursive t as (select 1 as x union all select (SELECT x+1) from
	//    t where x < 3) select * from t;"); REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// use with recursive in table creation
	REQUIRE_NO_FAIL(con.Query("create table integers as with recursive t as (select 1 as x union select x+1 from t "
	                          "where x < 3) select * from t;"));

	// more complex uncorrelated subquery
	result = con.Query("with recursive t as (select (select min(x) from integers) as x union select x+1 from t where x "
	                   "< 3) select * from t order by x;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// aggregate functions are not allowed in the recursive term of ctes
	REQUIRE_FAIL(
	    con.Query("with recursive t as (select 1 as x union select sum(x+1) from t where x < 3) select * from t"));

	// order by is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query(
	    "with recursive t as (select 1 as x union select sum(x+1) from t where x < 3 order by x) select * from t"));

	// limit is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query(
	    "with recursive t as (select 1 as x union select sum(x+1) from t where x < 3 LIMIT 1) select * from t"));

	// offset is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query(
	    "with recursive t as (select 1 as x union select sum(x+1) from t where x < 3 OFFSET 1) select * from t"));

	// offset is not allowed in the recursive term of ctes
	REQUIRE_FAIL(con.Query("with recursive t as (select 1 as x union select sum(x+1) from t where x < 3 LIMIT 1 OFFSET "
	                       "1) select * from t"));
}
