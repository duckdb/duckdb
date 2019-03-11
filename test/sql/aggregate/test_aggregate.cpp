#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test GROUP BY on expression", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integer(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integer VALUES (3, 4), (3, 5), (3, 7);"));
	result = con.Query("SELECT j * 2 FROM integer GROUP BY j * 2 ORDER BY j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
}
