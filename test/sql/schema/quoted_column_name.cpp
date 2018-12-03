
#include "catch.hpp"
#include "test_helpers.hpp"

#include "common/file_system.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test quoted column name", "[schema]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(\"42\" INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (33)"));
	result = con.Query("SELECT \"42\" FROM integers;");
	REQUIRE(CHECK_COLUMN(result, 0, {33}));
}
