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

TEST_CASE("Rounding test", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE roundme(a DOUBLE, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO roundme VALUES (42.123456, 3)"));

	result = con.Query("select round(42.12345, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {42.0}));

	result = con.Query("select round(42.12345, 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {42.12}));

	result = con.Query("select round(42, 0)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("select round(a, 1) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {42.1}));

	result = con.Query("select round(b, 1) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("select round(a, b) from roundme");
	REQUIRE(CHECK_COLUMN(result, 0, {42.123}));
}

// see https://www.postgresql.org/docs/10/functions-math.html

TEST_CASE("Function test cases from PG docs", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("select abs(-17.4)");
	REQUIRE(CHECK_COLUMN(result, 0, {17.4}));

	result = con.Query("select cbrt(27.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {3.0}));

	result = con.Query("select ceil(-42.8)");
	REQUIRE(CHECK_COLUMN(result, 0, {-42.0}));

	result = con.Query("select ceiling(-95.3)");
	REQUIRE(CHECK_COLUMN(result, 0, {-95.0}));

	result = con.Query("select exp(1.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.71828182845905}));

	result = con.Query("select floor(-42.8)");
	REQUIRE(CHECK_COLUMN(result, 0, {-43.0}));

	result = con.Query("select ln(2.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {0.693147180559945}));

	result = con.Query("select log(100.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.0}));

	result = con.Query("select log10(100.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.0}));

	result = con.Query("select log2(4.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {2.0}));

	result = con.Query("select pi()");
	REQUIRE(CHECK_COLUMN(result, 0, {3.14159265358979}));

	result = con.Query("select sqrt(2.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {1.4142135623731}));

	result = con.Query("select radians(45.0)");
	REQUIRE(CHECK_COLUMN(result, 0, {0.785398163397448}));

	result = con.Query("select degrees(0.5)");
	REQUIRE(CHECK_COLUMN(result, 0, {28.6478897565412}));

        result = con.Query("select sign(4.1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("select sign(-4.1)");
	REQUIRE(CHECK_COLUMN(result, 0, {-1}));

	result = con.Query("select sign(0)");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	result = con.Query("select sign(3)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}
