#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test prepared statements", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	auto prep = con.PrepareStatement("SELECT cast(? as integer)");
	prep->Bind(0, Value::INTEGER(42));
	result = prep->Execute(con);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}
