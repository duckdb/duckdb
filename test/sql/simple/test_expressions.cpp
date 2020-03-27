#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Complex Expressions", "[sql]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE exprtest (a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO exprtest VALUES (42, 10), (43, 100), (NULL, 1), (45, -1)"));

	result = con.Query("SELECT * FROM exprtest");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43, Value(), 45}));

	// BETWEEN
	result = con.Query("SELECT a FROM exprtest WHERE a BETWEEN 43 AND 44");
	REQUIRE(CHECK_COLUMN(result, 0, {43}));

	result = con.Query("SELECT a FROM exprtest WHERE a NOT BETWEEN 43 AND 44");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 45}));

	result = con.Query("SELECT a FROM exprtest WHERE a BETWEEN b AND 44");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// CASE
	result = con.Query("SELECT CASE a WHEN 42 THEN 100 WHEN 43 THEN 200 ELSE "
	                   "300 END FROM exprtest");
	REQUIRE(CHECK_COLUMN(result, 0, {100, 200, 300, 300}));

	result = con.Query("SELECT CASE WHEN a = 42 THEN 100 WHEN a = 43 THEN 200 "
	                   "ELSE 300 END FROM exprtest");
	REQUIRE(CHECK_COLUMN(result, 0, {100, 200, 300, 300}));

	result = con.Query("SELECT CASE WHEN a = 42 THEN 100 WHEN a = 43 THEN 200 "
	                   "END FROM exprtest");
	REQUIRE(CHECK_COLUMN(result, 0, {100, 200, Value(), Value()}));

	// COALESCE
	result = con.Query("SELECT COALESCE(NULL, NULL, 42, 43)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT COALESCE(NULL, NULL, 42)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT COALESCE(42, NULL, 43)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT COALESCE(NULL, NULL, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT COALESCE(a, b) FROM exprtest");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43, 1, 45}));

	// ABS
	result = con.Query("SELECT ABS(1), ABS(-1), ABS(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));

	result = con.Query("SELECT ABS(b) FROM exprtest");
	REQUIRE(CHECK_COLUMN(result, 0, {10, 100, 1, 1}));

	// IN

	con.Query("CREATE TABLE intest (a INTEGER, b INTEGER, c INTEGER)");

	con.Query("INSERT INTO intest VALUES (42, 42, 42), (43, 42, 42), (44, 41, 44);");

	result = con.Query("SELECT * FROM intest WHERE a IN (42, 43)");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 42}));
	REQUIRE(CHECK_COLUMN(result, 2, {42, 42}));

	result = con.Query("SELECT a IN (42, 43) FROM intest ");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 0}));

	result = con.Query("SELECT * FROM intest WHERE a IN (86, 103, 162)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));

	result = con.Query("SELECT * FROM intest WHERE a IN (NULL, NULL, NULL, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));

	result = con.Query("SELECT * FROM intest WHERE a IN (b)");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));
	REQUIRE(CHECK_COLUMN(result, 2, {42}));

	result = con.Query("SELECT * FROM intest WHERE a IN (b, c)");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 44}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 41}));
	REQUIRE(CHECK_COLUMN(result, 2, {42, 44}));

	result = con.Query("SELECT * FROM intest WHERE a IN (43, b) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 42}));
	REQUIRE(CHECK_COLUMN(result, 2, {42, 42}));

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (42, 43)");
	REQUIRE(CHECK_COLUMN(result, 0, {44}));
	REQUIRE(CHECK_COLUMN(result, 1, {41}));
	REQUIRE(CHECK_COLUMN(result, 2, {44}));

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (86, 103, 162) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 43, 44}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 42, 41}));
	REQUIRE(CHECK_COLUMN(result, 2, {42, 42, 44}));

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (NULL, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (b) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {43, 44}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 41}));
	REQUIRE(CHECK_COLUMN(result, 2, {42, 44}));

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (b, c)");
	REQUIRE(CHECK_COLUMN(result, 0, {43}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));
	REQUIRE(CHECK_COLUMN(result, 2, {42}));

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (43, b)");
	REQUIRE(CHECK_COLUMN(result, 0, {44}));
	REQUIRE(CHECK_COLUMN(result, 1, {41}));
	REQUIRE(CHECK_COLUMN(result, 2, {44}));

	result = con.Query("SELECT * FROM intest WHERE NULL IN ('a', 'b')");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));

	result = con.Query("SELECT * FROM intest WHERE NULL NOT IN ('a', 'b')");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	REQUIRE(CHECK_COLUMN(result, 2, {}));

	con.Query("CREATE TABLE strtest (a INTEGER, b VARCHAR)");
	con.Query("INSERT INTO strtest VALUES (1, 'a'), (2, 'h'), (3, 'd')");

	con.Query("INSERT INTO strtest VALUES (4, NULL)");

	result = con.Query("SELECT a FROM strtest WHERE b = 'a'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT a FROM strtest WHERE b <> 'a'");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));

	result = con.Query("SELECT a FROM strtest WHERE b < 'h'");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));

	result = con.Query("SELECT a FROM strtest WHERE b <= 'h'");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	result = con.Query("SELECT a FROM strtest WHERE b > 'h'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.Query("SELECT a FROM strtest WHERE b >= 'h'");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}
