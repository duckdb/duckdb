#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StatsBindData : public FunctionData {
	explicit StatsBindData(string stats_p = string()) : stats(move(stats_p)) {
	}

	string stats;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<StatsBindData>(stats);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const StatsBindData &)other_p;
		return stats == other.stats;
	}
};

static void StatsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (StatsBindData &)*func_expr.bind_info;
	if (info.stats.empty()) {
		info.stats = "No statistics";
	}
	Value v(info.stats);
	result.Reference(v);
}

unique_ptr<FunctionData> StatsBind(ClientContext &context, ScalarFunction &bound_function,
                                   vector<unique_ptr<Expression>> &arguments) {
	return make_unique<StatsBindData>();
}

static unique_ptr<BaseStatistics> StatsPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;
	if (child_stats[0]) {
		auto &info = (StatsBindData &)*bind_data;
		info.stats = child_stats[0]->ToString();
	}
	return nullptr;
}

void StatsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("stats", {LogicalType::ANY}, LogicalType::VARCHAR, StatsFunction, true, StatsBind,
	                               nullptr, StatsPropagateStats));
}

} // namespace duckdb
