#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test correlated subqueries", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE item(i_manufact INTEGER)"));

	REQUIRE_NO_FAIL(con.Query(
	    "SELECT * FROM item i1 WHERE (SELECT count(*) AS item_cnt FROM item WHERE (i_manufact = i1.i_manufact AND "
	    "i_manufact=3) OR (i_manufact = i1.i_manufact AND i_manufact=3)) > 0 ORDER BY 1 LIMIT 100;"));
	REQUIRE_NO_FAIL(con.Query(
	    "SELECT * FROM item i1 WHERE (SELECT count(*) AS item_cnt FROM item WHERE (i_manufact = i1.i_manufact AND "
	    "i_manufact=3) OR (i_manufact = i1.i_manufact AND i_manufact=3)) ORDER BY 1 LIMIT 100;"));
}
