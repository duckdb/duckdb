#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// Cast registration is a C++-only API, so the histogram bind deferring to registered casts cannot be
// exercised through SQL tests.

//! Wrap every source row into a single-element outer list
static bool WrapInListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	for (idx_t i = 0; i < count; i++) {
		auto val = source.GetValue(i);
		if (val.IsNull()) {
			result.SetValue(i, Value(result.GetType()));
		} else {
			result.SetValue(i, Value::LIST(source.GetType(), {std::move(val)}));
		}
	}
	return true;
}

TEST_CASE("Histogram bin validation defers to registered casts", "[api][histogram]") {
	DuckDB db;
	Connection con(db);
	// histogram(arg, bins) lives in core_functions - skip when it is not linked in
	if (con.Query("SELECT histogram(1, [1])")->HasError()) {
		return;
	}

	// without a registered cast the bins argument is rejected at bind time
	auto rejected = con.Query("SELECT histogram(l, b) FROM (VALUES ([1], [1]), ([2], [1])) t(l, b)");
	REQUIRE(rejected->HasError());
	REQUIRE(StringUtil::Contains(rejected->GetError(), "bin boundaries must match the input type"));

	// a registered INTEGER[] -> INTEGER[][] cast makes the same call executable
	auto &casts = DBConfig::GetConfig(*con.context).GetCastFunctions();
	casts.RegisterCastFunction(LogicalType::LIST(LogicalType::INTEGER),
	                           LogicalType::LIST(LogicalType::LIST(LogicalType::INTEGER)),
	                           BoundCastInfo(WrapInListCast));
	auto result = con.Query(
	    "SELECT list_aggregate(map_values(histogram(l, b)), 'sum') FROM (VALUES ([1], [1]), ([2], [1])) t(l, b)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}

TEST_CASE("Histogram bin validation accepts registered casts nested in the bins list", "[api][histogram]") {
	DuckDB db;
	Connection con(db);
	if (con.Query("SELECT histogram(1, [1])")->HasError()) {
		return;
	}

	// structs without matching member names cannot be cast by default - the struct cast bind itself throws
	auto rejected = con.Query("SELECT histogram(s, b) FROM (VALUES ({'b': 1}, [{'a': 1}])) t(s, b)");
	REQUIRE(rejected->HasError());
	REQUIRE(StringUtil::Contains(rejected->GetError(), "STRUCT to STRUCT cast must have at least one matching member"));

	// a registered cast between the struct types is picked up by the forced list cast
	auto source_type = LogicalType::STRUCT({{"a", LogicalType::INTEGER}});
	auto target_type = LogicalType::STRUCT({{"b", LogicalType::INTEGER}});
	auto &casts = DBConfig::GetConfig(*con.context).GetCastFunctions();
	casts.RegisterCastFunction(source_type, target_type, BoundCastInfo(DefaultCasts::ReinterpretCast));
	auto result = con.Query(
	    "SELECT list_aggregate(map_values(histogram(s, b)), 'sum') FROM (VALUES ({'b': 1}, [{'a': 1}])) t(s, b)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}
