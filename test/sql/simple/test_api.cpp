#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test fetch API", "[api][.]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	// con.EnableQueryVerification();
	unique_ptr<DuckDBResult> result;
	unique_ptr<DuckDBStreamingResult> streaming_result;

	streaming_result = con.SendQuery("CREATE TABLE test (a INTEGER);");
	// omit Close() here

	result = con.Query("select a from test where 1 <> 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	streaming_result = con.SendQuery("INSERT INTO test VALUES (42)");
	REQUIRE(streaming_result->Close());

	streaming_result = con.SendQuery("SELECT a from test");
	REQUIRE(result);

	REQUIRE(streaming_result->Fetch()->GetVector(0).GetValue(0) == Value::INTEGER(42));
	REQUIRE(streaming_result->Close());

	result = con.Query("select a from test");
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(42));
}
