#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple parallelism", "[parallelism]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=4"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// perform an operator with many pipelines
	result = con.Query("SELECT SUM(i) FROM integers UNION ALL SELECT AVG(i) FROM integers UNION ALL SELECT MIN(i) FROM integers UNION ALL SELECT MAX(i) FROM integers;");
	REQUIRE(CHECK_COLUMN(result, 0, {6.0, 2.0, 1.0, 3.0}));

	// errors in separate pipelines
	REQUIRE_FAIL(con.Query("SELECT SUM(i) FROM integers UNION ALL SELECT AVG(i) FROM integers UNION ALL SELECT MIN(i::DATE) FROM integers UNION ALL SELECT MAX(i::DATE) FROM integers;"));
}
