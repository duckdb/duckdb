#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("NOT NULL constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER NOT NULL)"));
	// insert normal value
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3)"));
	// insert NULL
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (NULL)"));

	// update normal value
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=4"));
	// update NULL
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=NULL"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers_with_null(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers_with_null VALUES (3), (4), (5), (NULL);"));

	//! INSERT from SELECT with NULL
	REQUIRE_FAIL(con.Query("INSERT INTO integers (i) SELECT * FROM integers_with_null"));
	//! INSERT from SELECT without NULL
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers (i) SELECT * FROM "
	                          "integers_with_null WHERE i IS NOT NULL"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 4, 5}));

	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=4 WHERE i>4"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 4, 4}));
}
