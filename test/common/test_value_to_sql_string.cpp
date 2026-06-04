#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

// Evaluates `SELECT <expr>` and returns the resulting scalar Value.
static Value EvalScalar(Connection &con, const string &expr) {
	auto result = con.Query("SELECT " + expr);
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0);
}

// Asserts the round-trip contract of Value::ToSQLString(): rendering a Value to
// SQL and re-parsing it yields a Value with the same type and contents.
static void RequireRoundTrip(Connection &con, const string &expr) {
	auto original = EvalScalar(con, expr);
	auto sql = original.ToSQLString();
	INFO("expr=" << expr << " ToSQLString=" << sql);
	auto roundtripped = EvalScalar(con, sql);
	REQUIRE(roundtripped.type() == original.type());
	REQUIRE(roundtripped.ToString() == original.ToString());
}

TEST_CASE("Value::ToSQLString round-trips MAP values", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// flat string-keyed map
	RequireRoundTrip(con, "MAP {'a': 'b', 'c': 'd'}");
	// integer-keyed / integer-valued map
	RequireRoundTrip(con, "MAP {1: 10, 2: 20}");
	// empty map needs an explicit cast to keep its element types
	RequireRoundTrip(con, "MAP {}::MAP(VARCHAR, VARCHAR)");
	// nested map
	RequireRoundTrip(con, "MAP {'a': MAP {'b': 1}}");
	// map with LIST values
	RequireRoundTrip(con, "MAP {'a': [1, 2, 3]}");
	// map with STRUCT values
	RequireRoundTrip(con, "MAP {'a': {'x': 1, 'y': 2}}");
	// map with a NULL value
	RequireRoundTrip(con, "MAP {'k': NULL}::MAP(VARCHAR, VARCHAR)");
	// keys/values that require single-quote escaping
	RequireRoundTrip(con, "MAP {'it''s': 'a''b'}");
	// the explicit cast also preserves narrower integer subtypes
	RequireRoundTrip(con, "MAP {1::SMALLINT: 2::SMALLINT}");
}
