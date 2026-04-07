#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Test function-only scalar functions still execute with dictionary optimization", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("PRAGMA debug_verify_vector='dictionary_expression'"));
	auto result = con.Query("SELECT length(s) FROM (VALUES ('aa'), ('bbb'), ('cccc')) tbl(s) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	REQUIRE_NO_FAIL(con.Query("PRAGMA debug_verify_vector='none'"));
}
