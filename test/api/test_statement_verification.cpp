#include "catch.hpp"
#include "compare_result.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/query_parameters.hpp"

using namespace duckdb;

namespace {

//! One debug_verify_statement mode, run against a result the caller is allowed to stream. A mode that
//! runs a statement of its own has to leave no query in flight behind it, or the statement being
//! verified cannot start: verification happens after InitialCleanup, so nothing else releases one.
void RunVerifiedStreamingQuery(const string &mode) {
	DuckDB db(nullptr);
	Connection con(db);

	auto set_result = con.Query("SET debug_verify_statement='" + mode + "'");
	REQUIRE(!set_result->HasError());

	auto result = con.SendQuery("SELECT 42", QueryParameters(true));
	REQUIRE(result);
	INFO(mode << ": " << (result->HasError() ? result->GetError() : string()));
	REQUIRE(!result->HasError());

	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	REQUIRE(chunk->GetValue(0, 0) == Value::INTEGER(42));

	// and the connection is usable afterwards
	auto after = con.Query("SELECT 43");
	REQUIRE(!after->HasError());
	REQUIRE(CHECK_COLUMN(after, 0, {Value::INTEGER(43)}));
}

} // namespace

TEST_CASE("Statement verification leaves no query in flight", "[api]") {
	RunVerifiedStreamingQuery("copy_statement");
	RunVerifiedStreamingQuery("reparse_statement");
	RunVerifiedStreamingQuery("serialize_statement");
	RunVerifiedStreamingQuery("explain_statement");
	RunVerifiedStreamingQuery("prepared_statement");
}
