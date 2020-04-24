#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test strip accents function", "[collate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	result = con.Query("SELECT strip_accents('hello'), strip_accents('héllo')");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello"}));

	result = con.Query("SELECT strip_accents('mühleisen'), strip_accents('hannes mühleisen')");
	REQUIRE(CHECK_COLUMN(result, 0, {"muhleisen"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hannes muhleisen"}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR, str VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('äää', 'aaa')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('hännës mühlëïsën', 'hannes muhleisen')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('olá', 'ola')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('ôâêóáëòõç', 'oaeoaeooc')"));

	result = con.Query("SELECT strip_accents(s)=strip_accents(str) FROM collate_test");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true, true}));
}
