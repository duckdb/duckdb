#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StatsBindData : public FunctionData {
	explicit StatsBindData(string stats_p = string()) : stats(std::move(stats_p)) {
	}

	string stats;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StatsBindData>(stats);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StatsBindData>();
		return stats == other.stats;
	}
};

static void StatsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<StatsBindData>();
	if (info.stats.empty()) {
		info.stats = "No statistics";
	}
	Value v(info.stats);
	result.Reference(v);
}

unique_ptr<FunctionData> StatsBind(ClientContext &context, ScalarFunction &bound_function,
                                   vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<StatsBindData>();
}

static unique_ptr<BaseStatistics> StatsPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;
	auto &info = bind_data->Cast<StatsBindData>();
	info.stats = child_stats[0].ToString();
	return nullptr;
}

ScalarFunction StatsFun::GetFunction() {
	ScalarFunction stats({LogicalType::ANY}, LogicalType::VARCHAR, StatsFunction, StatsBind, nullptr,
	                     StatsPropagateStats);
	stats.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	stats.stability = FunctionStability::VOLATILE;
	return stats;
}

} // namespace duckdb
