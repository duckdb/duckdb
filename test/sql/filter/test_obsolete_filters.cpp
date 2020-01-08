#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test expressions with obsolete filters", "[filter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO integers VALUES (1, 10), (2, 12), (3, 14), (4, 16), (5, NULL), (NULL, NULL)"));

	// Obsolete filters that can be pruned
	result = con.Query("SELECT * FROM integers WHERE TRUE ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 10, 12, 14, 16, Value()}));
	result = con.Query("SELECT * FROM integers WHERE FALSE ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	result = con.Query("SELECT * FROM integers WHERE NULL ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));
	// involving equality
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a>0");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE a>0 AND a=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a<4");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE a<4 AND a=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a<=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a>=2");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12}));
	// involving multiple GREATER THAN expressions
	result = con.Query("SELECT * FROM integers WHERE a>2 AND a>4");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	result = con.Query("SELECT * FROM integers WHERE a>4 AND a>2");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	result = con.Query("SELECT * FROM integers WHERE a>4 AND a>=4");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	result = con.Query("SELECT * FROM integers WHERE a>=4 AND a>4");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	// involving multiple LESS THAN expressions
	result = con.Query("SELECT * FROM integers WHERE a<2 AND a<4");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	result = con.Query("SELECT * FROM integers WHERE a<4 AND a<2");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	result = con.Query("SELECT * FROM integers WHERE a<2 AND a<=2");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	result = con.Query("SELECT * FROM integers WHERE a<=2 AND a<2");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	// involving inequality expression
	result = con.Query("SELECT * FROM integers WHERE a<2 AND a<>3");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	result = con.Query("SELECT * FROM integers WHERE a<=1 AND a<>3");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	result = con.Query("SELECT * FROM integers WHERE a>4 AND a<>2");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	result = con.Query("SELECT * FROM integers WHERE a>=5 AND a<>2");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	result = con.Query("SELECT * FROM integers WHERE a>=4 AND a<>4 AND a<>4");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// many conditions
	result = con.Query("SELECT * FROM integers WHERE a<3 AND a<4 AND a<5 AND a<10 AND a<2 AND a<20");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));

	/////////////////////////////////////////////////////////
	// Obsolete filters that always result in zero results //
	/////////////////////////////////////////////////////////
	// (i.e. entire tree can be pruned)
	// involving equality
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a=4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// greater than and equality
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a>4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a>4 AND a=2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a>2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a>=4 AND a=2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// less than and equality
	result = con.Query("SELECT * FROM integers WHERE a=4 AND a<2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a<2 AND a=4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a<2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a<=2 AND a=4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// less than and greater than
	result = con.Query("SELECT * FROM integers WHERE a<2 AND a>4");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// inequality
	result = con.Query("SELECT * FROM integers WHERE a=2 AND a<>2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a<>2 AND a=2");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// WHERE clause with explicit FALSE
	result = con.Query("SELECT * FROM integers WHERE 0");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM integers WHERE a<2 AND 0");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}

TEST_CASE("Test string expressions with obsolete filters", "[filter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello'), ('world'), (NULL)"));

	// equality
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s='world'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// inequality
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s<>'hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s<>'world'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	// range queries
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s>'a'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s>='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s<='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s<'z'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	// range queries with ezro results
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s<='a'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s<'hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s>'hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM strings WHERE s='hello' AND s>='z'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// range queries with inequality
	result = con.Query("SELECT * FROM strings WHERE s<>'hello' AND s<='a'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM strings WHERE s<>'hello' AND s<'hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM strings WHERE s<>'hello' AND s>'hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"world"}));
	result = con.Query("SELECT * FROM strings WHERE s<>'world' AND s>='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
}
