#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

// This test that the parser keeps statements in the right order, also when a multi-statement (i.e. a PIVOT is included
// See https://github.com/duckdb/duckdb/issues/18710
TEST_CASE("Parsing multiple statements including a PIVOT in one go results in a correctly ordered list", "[pivot][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto query = "PIVOT (SELECT 'a' AS col) ON col using first(col);SELECT 42;";

	// Check that the SELECT 42 statement is last in the parsed statements list
	auto statements = con.context->ParseStatements(query);
	REQUIRE(statements.size() == 3);
	// REQUIRE(statements.back()->query == "SELECT 42;");

	// Execute the query
	auto result = con.Query(query);

	// We expect two results
	REQUIRE(result);
	REQUIRE(result->next);
	REQUIRE(result->next->next == nullptr);
	// The first result should be that of the PIVOT statement
	REQUIRE(CHECK_COLUMN(result, 0, {"a"}));
	// The second result should be 42
	REQUIRE(CHECK_COLUMN(result->next, 0, {42}));
}
