#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundOperatorExpression &expr,
                                                                     unique_ptr<Expression> *expr_ptr) {
	bool all_have_stats = true;
	vector<unique_ptr<BaseStatistics>> child_stats;
	child_stats.reserve(expr.children.size());
	for (auto &child : expr.children) {
		auto stats = PropagateExpression(child);
		if (!stats) {
			all_have_stats = false;
		}
		child_stats.push_back(move(stats));
	}
	if (!all_have_stats) {
		return nullptr;
	}
	switch (expr.type) {
	case ExpressionType::OPERATOR_IS_NULL:
		if (!child_stats[0]->CanHaveNull()) {
			// child has no null values: x IS NULL will always be false
			*expr_ptr = make_unique<BoundConstantExpression>(Value::BOOLEAN(false));
			return PropagateExpression(*expr_ptr);
		}
		return nullptr;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		if (!child_stats[0]->CanHaveNull()) {
			// child has no null values: x IS NOT NULL will always be true
			*expr_ptr = make_unique<BoundConstantExpression>(Value::BOOLEAN(true));
			return PropagateExpression(*expr_ptr);
		}
		return nullptr;
	default:
		return nullptr;
	}
}

} // namespace duckdb
