#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test join on aggregates", "[join]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	result = con.Query("SELECT * FROM (SELECT SUM(i) AS x FROM integers) a, (SELECT SUM(i) AS x FROM integers) b WHERE a.x=b.x;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));
}
