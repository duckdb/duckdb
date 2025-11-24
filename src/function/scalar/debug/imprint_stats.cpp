#include "duckdb/function/scalar/debug/imprint_stats.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

static void ImprintStatsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();
	auto result_data = FlatVector::GetData<int64_t>(result);

	// get the statistic name from the input
	D_ASSERT(input.ColumnCount() == 1);
	auto &name_vec = input.data[0];

	UnifiedVectorFormat name_format;
	name_vec.ToUnifiedFormat(count, name_format);

	for (idx_t i = 0; i < count; i++) {
		auto name_idx = name_format.sel->get_index(i);
		if (!name_format.validity.RowIsValid(name_idx)) {
			result.SetValue(i, Value::BIGINT(0));
			continue;
		}

		auto name = StringValue::Get(name_vec.GetValue(name_idx));

		// get the requested statistic or reset
		int64_t value = 0;
		if (name == "imprint_checks_total") {
			value = static_cast<int64_t>(NumericStats::GetImprintChecksTotal());
		} else if (name == "imprint_pruned_segments") {
			value = static_cast<int64_t>(NumericStats::GetImprintPrunedSegments());
		} else if (name == "imprint_equality_checks") {
			value = static_cast<int64_t>(NumericStats::GetImprintEqualityChecks());
		} else if (name == "imprint_greater_than_checks") {
			value = static_cast<int64_t>(NumericStats::GetImprintGreaterThanChecks());
		} else if (name == "imprint_less_than_checks") {
			value = static_cast<int64_t>(NumericStats::GetImprintLessThanChecks());
		} else if (name == "total_segments_checked") {
			value = static_cast<int64_t>(NumericStats::GetTotalSegmentsChecked());
		} else if (name == "total_segments_skipped") {
			value = static_cast<int64_t>(NumericStats::GetTotalSegmentsSkipped());
		} else if (name == "reset") {
			// reset all statistics
			NumericStats::ResetImprintStatistics();
			value = 1;
		} else {
			// unkown stats
			value = 0;
		}

		result_data[i] = value;
	}

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction ImprintStatsFun::GetFunction() {
	return ScalarFunction("imprint_stats", {LogicalType::VARCHAR}, LogicalType::BIGINT, ImprintStatsFunction);
}

} // namespace duckdb
