#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ordering the same value several times", "[order][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	int64_t count = 4;
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (0), (0), (0), (0)"));

	while(count <= 16384*4) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT * FROM integers"));
		count += count;
	}

	// order by
	result = con.Query("SELECT SUM(i) FROM (SELECT i FROM integers ORDER BY i) t1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}
