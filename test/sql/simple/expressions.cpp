#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Complex Expressions", "[sql]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE exprtest (a INTEGER, b INTEGER)");
	con.Query("INSERT INTO exprtest VALUES (42, 10)");
	con.Query("INSERT INTO exprtest VALUES (43, 100)");
	con.Query("INSERT INTO exprtest VALUES (NULL, 1)");
	con.Query("INSERT INTO exprtest VALUES (45, -1)");

	result = con.Query("SELECT * FROM exprtest");
	CHECK_COLUMN(result, 0, {42, 43, Value(), 45});

	// BETWEEN
	result = con.Query("SELECT a FROM exprtest WHERE a BETWEEN 43 AND 44");
	CHECK_COLUMN(result, 0, {43});

	result = con.Query("SELECT a FROM exprtest WHERE a NOT BETWEEN 43 AND 44");
	CHECK_COLUMN(result, 0, {42, 45});

	result = con.Query("SELECT a FROM exprtest WHERE a BETWEEN b AND 44");
	CHECK_COLUMN(result, 0, {42});

	// CASE
	result = con.Query("SELECT CASE a WHEN 42 THEN 100 WHEN 43 THEN 200 ELSE "
	                   "300 END FROM exprtest");
	CHECK_COLUMN(result, 0, {100, 200, 300, 300});

	result = con.Query("SELECT CASE WHEN a = 42 THEN 100 WHEN a = 43 THEN 200 "
	                   "ELSE 300 END FROM exprtest");
	CHECK_COLUMN(result, 0, {100, 200, 300, 300});

	result = con.Query("SELECT CASE WHEN a = 42 THEN 100 WHEN a = 43 THEN 200 "
	                   "END FROM exprtest");
	CHECK_COLUMN(result, 0, {100, 200, Value(), Value()});

	// COALESCE
	result = con.Query("SELECT COALESCE(NULL, NULL, 42, 43)");
	CHECK_COLUMN(result, 0, {42});

	result = con.Query("SELECT COALESCE(NULL, NULL, 42)");
	CHECK_COLUMN(result, 0, {42});

	result = con.Query("SELECT COALESCE(42, NULL, 43)");
	CHECK_COLUMN(result, 0, {42});

	result = con.Query("SELECT COALESCE(NULL, NULL, NULL)");
	CHECK_COLUMN(result, 0, {Value()});

	result = con.Query("SELECT COALESCE(a, b) FROM exprtest");
	CHECK_COLUMN(result, 0, {42, 43, 1, 45});

	// ABS
	result = con.Query("SELECT ABS(1), ABS(-1), ABS(NULL)");
	CHECK_COLUMN(result, 0, {1});
	CHECK_COLUMN(result, 1, {1});
	CHECK_COLUMN(result, 2, {Value()});

	result = con.Query("SELECT ABS(b) FROM exprtest");
	CHECK_COLUMN(result, 0, {10, 100, 1, 1});

	// IN

	con.Query("CREATE TABLE intest (a INTEGER, b INTEGER, c INTEGER)");

	con.Query("INSERT INTO intest VALUES (42, 42, 42);");
	con.Query("INSERT INTO intest VALUES (43, 42, 42);");
	con.Query("INSERT INTO intest VALUES (44, 41, 44);");

	result = con.Query("SELECT * FROM intest WHERE a IN (42, 43)");
	CHECK_COLUMN(result, 0, {42, 43});
	CHECK_COLUMN(result, 1, {42, 42});
	CHECK_COLUMN(result, 2, {42, 42});

	result = con.Query("SELECT a IN (42, 43) FROM intest ");
	CHECK_COLUMN(result, 0, {1, 1, 0});

	result = con.Query("SELECT * FROM intest WHERE a IN (86, 103, 162)");
	CHECK_COLUMN(result, 0, {});
	CHECK_COLUMN(result, 1, {});
	CHECK_COLUMN(result, 2, {});

	result =
	    con.Query("SELECT * FROM intest WHERE a IN (NULL, NULL, NULL, NULL)");
	CHECK_COLUMN(result, 0, {});
	CHECK_COLUMN(result, 1, {});
	CHECK_COLUMN(result, 2, {});

	result = con.Query("SELECT * FROM intest WHERE a IN (b)");
	CHECK_COLUMN(result, 0, {42});
	CHECK_COLUMN(result, 1, {42});
	CHECK_COLUMN(result, 2, {42});

	result = con.Query("SELECT * FROM intest WHERE a IN (b, c)");
	CHECK_COLUMN(result, 0, {42, 44});
	CHECK_COLUMN(result, 1, {42, 41});
	CHECK_COLUMN(result, 2, {42, 44});

	result = con.Query("SELECT * FROM intest WHERE a IN (43, b)");
	CHECK_COLUMN(result, 0, {42, 43});
	CHECK_COLUMN(result, 1, {42, 42});
	CHECK_COLUMN(result, 2, {42, 42});

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (42, 43)");
	CHECK_COLUMN(result, 0, {44});
	CHECK_COLUMN(result, 1, {41});
	CHECK_COLUMN(result, 2, {44});

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (86, 103, 162)");
	CHECK_COLUMN(result, 0, {42, 43, 44});
	CHECK_COLUMN(result, 1, {42, 42, 41});
	CHECK_COLUMN(result, 2, {42, 42, 44});

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (NULL, NULL)");
	CHECK_COLUMN(result, 0, {});
	CHECK_COLUMN(result, 1, {});
	CHECK_COLUMN(result, 2, {});

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (b)");
	CHECK_COLUMN(result, 0, {43, 44});
	CHECK_COLUMN(result, 1, {42, 41});
	CHECK_COLUMN(result, 2, {42, 44});

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (b, c)");
	CHECK_COLUMN(result, 0, {43});
	CHECK_COLUMN(result, 1, {42});
	CHECK_COLUMN(result, 2, {42});

	result = con.Query("SELECT * FROM intest WHERE a NOT IN (43, b)");
	CHECK_COLUMN(result, 0, {44});
	CHECK_COLUMN(result, 1, {41});
	CHECK_COLUMN(result, 2, {44});

	con.Query("CREATE TABLE strtest (a INTEGER, b VARCHAR)");
	con.Query("INSERT INTO strtest VALUES (1, 'a')");
	con.Query("INSERT INTO strtest VALUES (2, 'h')");
	con.Query("INSERT INTO strtest VALUES (3, 'd')");
	con.Query("INSERT INTO strtest VALUES (4, NULL)");

	result = con.Query("SELECT a FROM strtest WHERE b = 'a'");
	CHECK_COLUMN(result, 0, {1});

	result = con.Query("SELECT a FROM strtest WHERE b <> 'a'");
	CHECK_COLUMN(result, 0, {2, 3});

	result = con.Query("SELECT a FROM strtest WHERE b < 'h'");
	CHECK_COLUMN(result, 0, {1, 3});

	result = con.Query("SELECT a FROM strtest WHERE b <= 'h'");
	CHECK_COLUMN(result, 0, {1, 2, 3});

	result = con.Query("SELECT a FROM strtest WHERE b > 'h'");
	CHECK_COLUMN(result, 0, {});

	result = con.Query("SELECT a FROM strtest WHERE b >= 'h'");
	CHECK_COLUMN(result, 0, {2});
}
