#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

using namespace duckdb;
using Catch::Matchers::Contains;

namespace {

struct IsEvenOperator {
	static bool Operation(int32_t value) {
		return value % 2 == 0;
	}
};

static void IsEvenFunction(DataChunk &args, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);

	UnifiedVectorFormat data;
	args.data[0].ToUnifiedFormat(args.size(), data);
	auto input_data = UnifiedVectorFormat::GetData<int32_t>(data);
	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (data.validity.CanHaveNull()) {
		result_validity.Copy(data.validity, args.size());
	} else {
		result_validity.SetAllValid(args.size());
	}
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = data.sel->get_index(i);
		result_data[i] = data.validity.RowIsValid(idx) && IsEvenOperator::Operation(input_data[idx]);
	}
}

static idx_t IsEvenSelectFunction(DataChunk &args, ExpressionState &, SelectionVector *true_sel,
                                  SelectionVector *false_sel) {
	UnifiedVectorFormat data;
	args.data[0].ToUnifiedFormat(args.size(), data);
	auto input_data = UnifiedVectorFormat::GetData<int32_t>(data);

	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = data.sel->get_index(i);
		if (data.validity.RowIsValid(idx) && IsEvenOperator::Operation(input_data[idx])) {
			if (true_sel) {
				true_sel->set_index(true_count, i);
			}
			true_count++;
		} else if (false_sel) {
			false_sel->set_index(false_count++, i);
		}
	}
	return true_count;
}

static idx_t NullOrEvenSelectFunction(DataChunk &args, ExpressionState &, SelectionVector *true_sel,
                                      SelectionVector *false_sel) {
	UnifiedVectorFormat data;
	args.data[0].ToUnifiedFormat(args.size(), data);
	auto input_data = UnifiedVectorFormat::GetData<int32_t>(data);

	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = data.sel->get_index(i);
		if (!data.validity.RowIsValid(idx) || IsEvenOperator::Operation(input_data[idx])) {
			if (true_sel) {
				true_sel->set_index(true_count, i);
			}
			true_count++;
		} else if (false_sel) {
			false_sel->set_index(false_count++, i);
		}
	}
	return true_count;
}

static void RegisterScalarFunction(Connection &con, ScalarFunction function) {
	CreateScalarFunctionInfo info(std::move(function));
	info.schema = DEFAULT_SCHEMA;
	con.context->RegisterFunction(info);
}

} // namespace

TEST_CASE("Test scalar function select hook fallback paths", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	RegisterScalarFunction(
	    con, ScalarFunction("is_even_function_only", {LogicalType::INTEGER}, LogicalType::BOOLEAN, IsEvenFunction));

	ScalarFunction select_only("is_even_select_only", {LogicalType::INTEGER}, LogicalType::BOOLEAN, nullptr);
	select_only.SetSelectCallback(IsEvenSelectFunction);
	RegisterScalarFunction(con, std::move(select_only));

	auto projection_result = con.Query(
	    "SELECT i, is_even_select_only(i) AS is_even FROM (VALUES (0, 1), (1, 2), (2, NULL), (3, 4)) tbl(ord, i) "
	    "ORDER BY ord");
	REQUIRE(CHECK_COLUMN(projection_result, 0, {1, 2, Value(nullptr), 4}));
	REQUIRE(CHECK_COLUMN(projection_result, 1, {false, true, Value(nullptr), true}));

	auto function_only_filter =
	    con.Query("SELECT i FROM (VALUES (1), (2), (3), (4), (NULL)) tbl(i) WHERE is_even_function_only(i) ORDER BY i");
	REQUIRE(CHECK_COLUMN(function_only_filter, 0, {2, 4}));

	auto select_only_filter = con.Query(
	    "SELECT i FROM (VALUES (1), (2), (3), (4), (NULL)) tbl(i) WHERE i >= 2 AND is_even_select_only(i) ORDER BY i");
	REQUIRE(CHECK_COLUMN(select_only_filter, 0, {2, 4}));
}

TEST_CASE("Test scalar function select hook with special null handling", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	ScalarFunction select_only_special("null_or_even_select_only", {LogicalType::INTEGER}, LogicalType::BOOLEAN,
	                                   nullptr);
	select_only_special.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	select_only_special.SetSelectCallback(NullOrEvenSelectFunction);
	RegisterScalarFunction(con, std::move(select_only_special));

	auto filter_result =
	    con.Query("SELECT i FROM (VALUES (1), (2), (3), (4), (NULL)) tbl(i) WHERE null_or_even_select_only(i)");
	REQUIRE(CHECK_COLUMN(filter_result, 0, {2, 4, Value(nullptr)}));

	auto projection_result = con.Query("SELECT null_or_even_select_only(i) FROM (VALUES (1), (2), (NULL)) tbl(i)");
	REQUIRE_FAIL(projection_result);
	REQUIRE_THAT(projection_result->GetError(), Contains("only has a select callback with SPECIAL_HANDLING"));
}
