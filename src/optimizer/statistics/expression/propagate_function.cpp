#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundFunctionExpression &func,
                                                                     unique_ptr<Expression> *expr_ptr) {
	vector<unique_ptr<BaseStatistics>> stats;
	stats.reserve(func.children.size());
	for (idx_t i = 0; i < func.children.size(); i++) {
		stats.push_back(PropagateExpression(func.children[i]));
	}
	if (!func.function.statistics) {
		return nullptr;
	}
	FunctionStatisticsInput input(func, func.bind_info.get(), stats, expr_ptr);
	return func.function.statistics(context, input);
}

} // namespace duckdb
