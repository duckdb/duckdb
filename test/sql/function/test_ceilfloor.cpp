#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ceil(ing)/floor function", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(n DOUBLE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (NULL),(-42.8),(-42.2),(0), (42.2), (42.8)"));

	result = con.Query("SELECT cast(CEIL(n::tinyint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::smallint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::integer) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::bigint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(CEIL(n::float) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 43, 43}));
	result = con.Query("SELECT cast(CEIL(n::double) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 43, 43}));

	result = con.Query("SELECT cast(CEILING(n::double) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 43, 43}));

	result = con.Query("SELECT cast(FLOOR(n::tinyint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::smallint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::integer) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::bigint) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -42, -42, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::float) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -43, -43, 0, 42, 42}));
	result = con.Query("SELECT cast(FLOOR(n::double) as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -43, -43, 0, 42, 42}));
}
