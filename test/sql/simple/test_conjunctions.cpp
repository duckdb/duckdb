#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test conjunction statements", "[conjunction]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a (i integer, j integer);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (3, 4), (4, 5), (5, 6)"));

	result = con.Query("SELECT * FROM a WHERE (i > 3 AND j < 5) OR (i > 3 AND j > 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));

	// test boolean logic in conjunctions
	result = con.Query("SELECT true AND true");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT true AND false");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT false AND true");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT false AND false");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT false AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT NULL AND false");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT NULL AND true");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT true AND NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT true OR true");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT true OR NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT NULL OR true");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT false OR NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT NULL OR false");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT true OR false");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT false OR true");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT false OR false");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	// test single constant in conjunctions
	result = con.Query("SELECT true AND i>3 FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true}));
	result = con.Query("SELECT i>3 AND true FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true}));

	result = con.Query("SELECT 2>3 AND i>3 FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false}));
	result = con.Query("SELECT false AND i>3 FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false}));
	result = con.Query("SELECT i>3 AND false FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, false, false}));

	result = con.Query("SELECT false OR i>3 FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true}));
	result = con.Query("SELECT i>3 OR false FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true}));

	result = con.Query("SELECT true OR i>3 FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true}));
	result = con.Query("SELECT i>3 OR true FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true, true}));

	result = con.Query("SELECT NULL OR i>3 FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, true}));
	result = con.Query("SELECT i>3 OR NULL FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, true}));

	result = con.Query("SELECT NULL AND i>3 FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, Value(), Value()}));
	result = con.Query("SELECT i>3 AND NULL FROM a ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, Value(), Value()}));
}

TEST_CASE("Test conjunction statements that can be simplified", "[conjunction]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// test conjunctions in FILTER clause
	result = con.Query("SELECT i FROM integers WHERE (i=1 AND i>0) OR (i=1 AND i<3) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT i FROM integers WHERE (i=1) OR (i=1) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT i FROM integers WHERE (i=1) OR (i=1) OR (i=1) OR (i=1) OR (i=1) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	result = con.Query("SELECT i FROM integers WHERE (i IS NULL AND i=1) OR (i IS NULL AND i<10) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.Query("SELECT i FROM integers WHERE (i IS NOT NULL AND i>1) OR (i IS NOT NULL AND i<10) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	result = con.Query(
	    "SELECT i FROM integers WHERE (i IS NULL AND (i+1) IS NULL) OR (i IS NULL AND (i+2) IS NULL) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	result = con.Query("SELECT i FROM integers WHERE i=1 OR 1=1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));

	result = con.Query("SELECT i FROM integers WHERE i=1 OR 1=0 OR 1=1 ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));

	result = con.Query("SELECT i FROM integers WHERE (i=1 OR 1=0 OR i=1) AND (0=1 OR 1=0 OR 1=1) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// test conjunctions in SELECT clause
	result = con.Query("SELECT (i=1 AND i>0) OR (i=1 AND i<3) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, false, false}));

	result = con.Query("SELECT (i=1) OR (i=1) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, false, false}));

	result = con.Query("SELECT (i=1) OR (i=1) OR (i=1) OR (i=1) OR (i=1) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), true, false, false}));

	result = con.Query("SELECT (i IS NULL AND i=1) OR (i IS NULL AND i<10) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, false}));

	result = con.Query("SELECT (i IS NOT NULL AND i>1) OR (i IS NOT NULL AND i<10) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {false, true, true, true}));

	result =
	    con.Query("SELECT (i IS NULL AND (i+1) IS NULL) OR (i IS NULL AND (i+2) IS NULL) FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {true, false, false, false}));
}
