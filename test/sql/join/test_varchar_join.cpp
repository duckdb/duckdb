#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test joins on VARCHAR columns with NULL values", "[join]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	result = con.Query("select * from (select 42 as a, NULL::varchar as b) sq1, (select 42 as a, 'asdf' as b) sq2 where sq1.b <> sq2.b;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.Query("select * from (select 42 as a, NULL::varchar as b) sq1, (select 42 as a, 'asdf' as b) sq2 where sq1.a=sq2.a and sq1.b <> sq2.b;");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
}
