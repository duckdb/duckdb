#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

using namespace duckdb;

// A concurrent append can grow a scanned column past the row count captured at scan init, so the
// filter can be handed a vector holding more rows than count. The caller sizes result_sel to count
// only, so a lookup that runs to the end of the vector writes past the end of result_sel. That race
// cannot be driven from SQL, so the bound is asserted directly here.
TEST_CASE("Prefix range filter lookup is bounded by the filter count", "[filter][prefix_range]") {
	constexpr idx_t VECTOR_ROWS = STANDARD_VECTOR_SIZE;
	constexpr idx_t FILTER_ROWS = VECTOR_ROWS >= 64 ? VECTOR_ROWS / 64 : 1;
	constexpr idx_t NULL_ROW = FILTER_ROWS - 1;
	constexpr idx_t MAX_BITS = 1ULL << 26;

	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;

	Vector keys(LogicalType::BIGINT, VECTOR_ROWS);
	auto key_data = FlatVector::GetDataMutable<int64_t>(keys);
	FlatVector::SetSize(keys, count_t(VECTOR_ROWS));

	for (idx_t i = 0; i < VECTOR_ROWS; i++) {
		key_data[i] = NumericCast<int64_t>(2 * i + 1);
	}
	auto filter = PrefixRangeFilter::CreatePrefixRangeFilter(LogicalType::BIGINT);
	filter->Initialize(context, VECTOR_ROWS, Value::BIGINT(1), Value::BIGINT(NumericCast<int64_t>(2 * VECTOR_ROWS - 1)),
	                   MAX_BITS);
	auto build_state = filter->InitializeBuildState(context);
	filter->InsertKeys(keys, *build_state);
	filter->MergeBuildState(*build_state);

	// only the odd keys are in the bitmap, key 0 sits below its lower bound, and NULLs never match
	for (idx_t i = 0; i < VECTOR_ROWS; i++) {
		key_data[i] = NumericCast<int64_t>(i);
	}
	FlatVector::SetNull(keys, NULL_ROW, true);

	vector<idx_t> expected_rows;
	for (idx_t i = 0; i < FILTER_ROWS; i++) {
		if (i % 2 == 1 && i != NULL_ROW) {
			expected_rows.push_back(i);
		}
	}
	SelectionVector result_sel;
	result_sel.Initialize(FILTER_ROWS);
	auto found_count = filter->LookupKeys(keys, result_sel, FILTER_ROWS);
	REQUIRE(found_count == expected_rows.size());
	for (idx_t i = 0; i < found_count; i++) {
		REQUIRE(result_sel.get_index(i) == expected_rows[i]);
	}

	// the selection overload reports positions in sel rather than the row ids it selected
	SelectionVector input_sel;
	input_sel.Initialize(FILTER_ROWS);
	for (idx_t i = 0; i < FILTER_ROWS; i++) {
		input_sel.set_index(i, FILTER_ROWS - i - 1);
	}
	vector<idx_t> expected_positions;
	for (idx_t i = 0; i < FILTER_ROWS; i++) {
		const auto row = FILTER_ROWS - i - 1;
		if (row % 2 == 1 && row != NULL_ROW) {
			expected_positions.push_back(i);
		}
	}
	SelectionVector selected_sel;
	selected_sel.Initialize(FILTER_ROWS);
	auto selected_count = filter->LookupKeys(keys, input_sel, selected_sel, FILTER_ROWS);
	REQUIRE(selected_count == expected_positions.size());
	for (idx_t i = 0; i < selected_count; i++) {
		REQUIRE(selected_sel.get_index(i) == expected_positions[i]);
	}
}

TEST_CASE("Prefix range filter prune handles all-NULL and missing statistics", "[filter][prefix_range]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;

	Vector keys(LogicalType::BIGINT, 2);
	auto key_data = FlatVector::GetDataMutable<int64_t>(keys);
	FlatVector::SetSize(keys, count_t(2));
	key_data[0] = 1;
	key_data[1] = 100;

	auto filter = PrefixRangeFilter::CreatePrefixRangeFilter(LogicalType::BIGINT);
	filter->Initialize(context, 2, Value::BIGINT(1), Value::BIGINT(100), 1ULL << 26);
	auto build_state = filter->InitializeBuildState(context);
	filter->InsertKeys(keys, *build_state);
	filter->MergeBuildState(*build_state);
	REQUIRE(filter->IsInitialized());

	auto make_expr = [](optional_ptr<PrefixRangeFilter> prf, bool filters_null_values) {
		vector<unique_ptr<Expression>> children;
		children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0ULL));
		return make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(PrefixRangeScalarFun::GetFunction(LogicalType::BIGINT)), std::move(children),
		    make_uniq<PrefixRangeFunctionData>(prf, filters_null_values, "k", LogicalType::BIGINT, 0.0f, 0ULL));
	};

	auto prune = [](BoundFunctionExpression &expr, optional_ptr<const BaseStatistics> stats) {
		vector<optional_ptr<const BaseStatistics>> child_stats;
		child_stats.push_back(stats);
		FunctionStatisticsPruneInput input(expr, expr.BindInfo().get(), child_stats);
		return PrefixRangeScalarFun::FilterPrune(input);
	};

	auto all_null = BaseStatistics::CreateEmpty(LogicalType::BIGINT);
	all_null.SetHasNull();
	auto unknown = BaseStatistics::CreateUnknown(LogicalType::BIGINT);

	auto null_rejecting = make_expr(filter.get(), true);
	REQUIRE(prune(*null_rejecting, &all_null) == FilterPropagateResult::FILTER_ALWAYS_FALSE);
	REQUIRE(prune(*null_rejecting, &unknown) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	REQUIRE(prune(*null_rejecting, nullptr) == FilterPropagateResult::NO_PRUNING_POSSIBLE);

	auto null_keeping = make_expr(filter.get(), false);
	REQUIRE(prune(*null_keeping, &all_null) == FilterPropagateResult::NO_PRUNING_POSSIBLE);

	auto uninitialized = PrefixRangeFilter::CreatePrefixRangeFilter(LogicalType::BIGINT);
	auto no_build = make_expr(uninitialized.get(), true);
	REQUIRE(prune(*no_build, &all_null) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
}
