#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test NULLS FIRST/NULLS LAST", "[orderby]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
    REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (NULL)"));

	// default is NULLS FIRST
    result = con.Query("SELECT * FROM integers ORDER BY i");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1}));
	result = con.Query("SELECT * FROM integers ORDER BY i NULLS FIRST");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1}));
    result = con.Query("SELECT * FROM integers ORDER BY i NULLS LAST");
    REQUIRE(CHECK_COLUMN(result, 0, {1, Value()}));

    result = con.Query("SELECT 10 AS j, i FROM integers ORDER BY j, i NULLS LAST");
    REQUIRE(CHECK_COLUMN(result, 0, {10, 10}));
    REQUIRE(CHECK_COLUMN(result, 1, {1, Value()}));

	// multiple columns with a mix
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER, j INTEGER)"));
    REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 1), (NULL, 1), (1, NULL)"));

	result = con.Query("SELECT * FROM test ORDER BY i NULLS FIRST, j NULLS LAST");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 1}));
    REQUIRE(CHECK_COLUMN(result, 1, {1, 1, Value()}));

    result = con.Query("SELECT * FROM test ORDER BY i NULLS FIRST, j NULLS FIRST");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 1}));
    REQUIRE(CHECK_COLUMN(result, 1, {1, Value(), 1}));

    result = con.Query("SELECT * FROM test ORDER BY i NULLS LAST, j NULLS FIRST");
    REQUIRE(CHECK_COLUMN(result, 0, {1, 1, Value()}));
    REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 1}));

	// now in window functions
	result = con.Query("SELECT i, j, row_number() OVER (PARTITION BY i ORDER BY j NULLS FIRST) FROM test ORDER BY i NULLS FIRST, j NULLS FIRST;");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 1}));
    REQUIRE(CHECK_COLUMN(result, 1, {1, Value(), 1}));
    REQUIRE(CHECK_COLUMN(result, 2, {1, 1, 2}));

    result = con.Query("SELECT i, j, row_number() OVER (PARTITION BY i ORDER BY j NULLS LAST) FROM test ORDER BY i NULLS FIRST, j NULLS FIRST;");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 1}));
    REQUIRE(CHECK_COLUMN(result, 1, {1, Value(), 1}));
    REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 1}));

	// TOP N
    result = con.Query("SELECT * FROM test ORDER BY i NULLS FIRST, j NULLS LAST LIMIT 2");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1}));
    REQUIRE(CHECK_COLUMN(result, 1, {1, 1}));
    result = con.Query("SELECT * FROM test ORDER BY i NULLS LAST, j NULLS LAST LIMIT 2");
    REQUIRE(CHECK_COLUMN(result, 0, {1, 1}));
    REQUIRE(CHECK_COLUMN(result, 1, {1, Value()}));
}

TEST_CASE("Test NULLS FIRST/NULLS LAST in config", "[orderby]") {
    unique_ptr<QueryResult> result;
	DBConfig config;
	config.default_null_order = OrderByNullType::NULLS_LAST;
    DuckDB db(nullptr, &config);
    Connection con(db);
    con.EnableQueryVerification();

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
    REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (NULL)"));

	// default is now NULLS LAST
    result = con.Query("SELECT * FROM integers ORDER BY i");
    REQUIRE(CHECK_COLUMN(result, 0, {1, Value()}));
    result = con.Query("SELECT * FROM integers ORDER BY i NULLS FIRST");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1}));
    result = con.Query("SELECT * FROM integers ORDER BY i NULLS LAST");
    REQUIRE(CHECK_COLUMN(result, 0, {1, Value()}));
}

