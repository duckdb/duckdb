#include <utility>

#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
class ClientContext;
class DataChunk;

namespace {
struct StatsBindData : public FunctionData {
	explicit StatsBindData(Value stats_p = Value(LogicalType::VARIANT())) : stats(std::move(stats_p)) {
	}

	Value stats;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StatsBindData>(stats);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StatsBindData>();
		return Value::NotDistinctFrom(stats, other.stats);
	}
};

void StatsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<StatsBindData>();
	result.Reference(info.stats);
}

unique_ptr<FunctionData> StatsBind(ClientContext &context, ScalarFunction &bound_function,
                                   vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<StatsBindData>();
}

unique_ptr<BaseStatistics> StatsPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;
	auto &info = bind_data->Cast<StatsBindData>();
	info.stats = child_stats[0].ToStruct().CastAs(context, LogicalType::VARIANT());
	return nullptr;
}

} // namespace

ScalarFunction StatsFun::GetFunction() {
	ScalarFunction stats({LogicalType::ANY}, LogicalType::VARIANT(), StatsFunction, StatsBind, nullptr,
	                     StatsPropagateStats);
	stats.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	stats.SetStability(FunctionStability::VOLATILE);
	return stats;
}

} // namespace duckdb
