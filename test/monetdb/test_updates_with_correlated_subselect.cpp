#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("MonetDB Test: update_with_correlated_subselect.SF-1284791.sql", "[monetdb]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table t1284791b (id2 int, val2 varchar(255))"));
	REQUIRE_NO_FAIL(con.Query("create table t1284791a (id1 int, val1 varchar(255))"));
	REQUIRE_NO_FAIL(con.Query("insert into t1284791a values (1,'1')"));
	REQUIRE_NO_FAIL(con.Query("insert into t1284791b values (1,'2')"));

	REQUIRE_NO_FAIL(con.Query("update t1284791a set val1 = (select val2 from t1284791b where id1 = id2) where id1 in "
	                          "(select id2 from t1284791b);"));

	result = con.Query("select * from t1284791a");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"2"}));

	result = con.Query("select * from t1284791b");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"2"}));

	REQUIRE_NO_FAIL(con.Query("drop table t1284791a;"));
	REQUIRE_NO_FAIL(con.Query("drop table t1284791b;"));
}
