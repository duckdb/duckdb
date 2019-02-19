#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test fetch API", "[api][.]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	// con.EnableQueryVerification();

	bool result;
	result = con.SendQuery("CREATE TABLE test (a INTEGER);");
	REQUIRE(result);
	result = con.CloseResult();
	REQUIRE(result);

	auto mat_res0 = con.Query("select a from test where 1 <> 1");
	REQUIRE(CHECK_COLUMN(mat_res0, 0, {}));

	result = con.SendQuery("INSERT INTO test VALUES (42)");
	REQUIRE(result);
	result = con.CloseResult();
	REQUIRE(result);
	result = con.SendQuery("SELECT a from test");
	REQUIRE(result);

	REQUIRE(con.FetchResultChunk()->GetVector(0).GetValue(0) == Value::INTEGER(42));
	result = con.CloseResult();
	REQUIRE(result);

	auto mat_res = con.Query("select a from test");
	REQUIRE(mat_res->GetValue(0, 0) == Value::INTEGER(42));
}
