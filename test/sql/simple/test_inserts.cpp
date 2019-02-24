#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test insert into statements", "[simpleinserts]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// big insert
	con.Query("CREATE TABLE integers(i INTEGER)");
	result = con.Query("INSERT INTO integers VALUES (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	                   "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	                   "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	                   "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	                   "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	                   "(4), (5), (6), (7), (8), (9)");
	REQUIRE(CHECK_COLUMN(result, 0, {1050}));

	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1050}));

	// insert into from SELECT
	result = con.Query("INSERT INTO integers SELECT * FROM integers;");
	REQUIRE(CHECK_COLUMN(result, 0, {1050}));

	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2100}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers;"));

	// insert into from query with column predicates
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));

	result = con.Query("INSERT INTO integers VALUES (3, 4), (4, 3);");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// insert into with default
	result = con.Query("INSERT INTO integers VALUES (DEFAULT, 4);");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("INSERT INTO integers (i) SELECT j FROM integers;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, Value(), 4, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 3, 4, Value(), Value(), Value()}));
}

TEST_CASE("Test insert into from wrong type", "[simpleinserts]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(a VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3), (4), (NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings SELECT * FROM integers"));

	result = con.Query("SELECT * FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("3"), Value("4"), Value()}));

	REQUIRE_NO_FAIL(con.Query("UPDATE strings SET a=13 WHERE a=3"));

	result = con.Query("SELECT * FROM strings ORDER BY cast(a AS INTEGER)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value("4"), Value("13")}));
}

TEST_CASE("Test insert from constant query", "[simpleinserts]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT 42"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT CAST(NULL AS VARCHAR)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {42, Value()}));
}

TEST_CASE("Test insert with invalid UTF8", "[simpleinserts]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(i STRING)"));
	REQUIRE_FAIL(con.Query("INSERT INTO strings VALUES ('\xe2\x82\x28')"));
	REQUIRE_FAIL(con.Query("SELECT * FROM strings WHERE i = '\xe2\x82\x28'"));
}

TEST_CASE("Test insert with too few or too many cols", "[simpleinserts]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i integer, j integer)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (1, 2)"));
	REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (1)"));
	REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (1,2,3)"));
	REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (1,2),(3)"));
	REQUIRE_FAIL(con.Query("INSERT INTO a VALUES (1,2),(3,4,5)"));
}
