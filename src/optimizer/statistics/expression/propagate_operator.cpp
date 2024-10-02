#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundOperatorExpression &expr,
                                                                     unique_ptr<Expression> &expr_ptr) {
	bool all_have_stats = true;
	vector<unique_ptr<BaseStatistics>> child_stats;
	child_stats.reserve(expr.children.size());
	for (auto &child : expr.children) {
		auto stats = PropagateExpression(child);
		if (!stats) {
			all_have_stats = false;
		}
		child_stats.push_back(std::move(stats));
	}
	if (!all_have_stats) {
		return nullptr;
	}
	switch (expr.type) {
	case ExpressionType::OPERATOR_COALESCE:
		// COALESCE, merge stats of all children
		for (idx_t i = 0; i < expr.children.size(); i++) {
			D_ASSERT(child_stats[i]);
			if (!child_stats[i]->CanHaveNoNull()) {
				// this child is always NULL, we can remove it from the coalesce
				// UNLESS there is only one node remaining
				if (expr.children.size() > 1) {
					expr.children.erase_at(i);
					child_stats.erase_at(i);
					i--;
				}
			} else if (!child_stats[i]->CanHaveNull()) {
				// coalesce child cannot have NULL entries
				// this is the last coalesce node that influences the result
				// we can erase any children after this node
				if (i + 1 < expr.children.size()) {
					expr.children.erase(expr.children.begin() + NumericCast<int64_t>(i + 1), expr.children.end());
					child_stats.erase(child_stats.begin() + NumericCast<int64_t>(i + 1), child_stats.end());
				}
				break;
			}
		}
		D_ASSERT(!expr.children.empty());
		D_ASSERT(expr.children.size() == child_stats.size());
		if (expr.children.size() == 1) {
			// coalesce of one entry: simply return that entry
			expr_ptr = std::move(expr.children[0]);
		} else {
			// coalesce of multiple entries
			// merge the stats
			for (idx_t i = 1; i < expr.children.size(); i++) {
				child_stats[0]->Merge(*child_stats[i]);
			}
		}
		return std::move(child_stats[0]);
	case ExpressionType::OPERATOR_IS_NULL:
		if (!child_stats[0]->CanHaveNull()) {
			// child has no null values: x IS NULL will always be false
			expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
			return PropagateExpression(expr_ptr);
		}
		if (!child_stats[0]->CanHaveNoNull()) {
			// child has no valid values: x IS NULL will always be true
			expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
			return PropagateExpression(expr_ptr);
		}
		return nullptr;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		if (!child_stats[0]->CanHaveNull()) {
			// child has no null values: x IS NOT NULL will always be true
			expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
			return PropagateExpression(expr_ptr);
		}
		if (!child_stats[0]->CanHaveNoNull()) {
			// child has no valid values: x IS NOT NULL will always be false
			expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
			return PropagateExpression(expr_ptr);
		}
		return nullptr;
	default:
		return nullptr;
	}
}

} // namespace duckdb
