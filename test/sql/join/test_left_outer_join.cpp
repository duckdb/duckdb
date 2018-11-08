
#include "catch.hpp"
#include "test_helpers.hpp"

#include "common/file_system.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test LEFT OUTER JOIN", "[join]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO integers VALUES (1, 2), (2, 3), (3, 4)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2(k INTEGER, l INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers2 VALUES (1, 10), (2, 20)"));

	result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON "
	                   "integers.i=integers2.k ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {10, 20, Value()}));
}
