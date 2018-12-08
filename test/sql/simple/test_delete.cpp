#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Deletions", "[delete]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42);"));

	// delete everything
	REQUIRE_NO_FAIL(con.Query("DELETE FROM a;"));

	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}
