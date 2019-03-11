#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test filter on alias", "[filter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// this fails in postgres and monetdb, but succeeds in sqlite
	// for now, we have this fail as well because it simplifies our life
	// the filter occurs before the projection, hence "j" is not computed until AFTER the filter normally
	REQUIRE_FAIL(con.Query("SELECT i % 2 AS j FROM integers WHERE j<>0;"));
}
