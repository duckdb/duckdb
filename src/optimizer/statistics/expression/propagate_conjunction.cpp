
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundConjunctionExpression &expr,
                                                                     unique_ptr<Expression> *expr_ptr) {
	auto is_and = expr.type == ExpressionType::CONJUNCTION_AND;
	for (idx_t expr_idx = 0; expr_idx < expr.children.size(); expr_idx++) {
		auto &child = expr.children[expr_idx];
		auto stats = PropagateExpression(child);
		if (!child->IsFoldable()) {
			continue;
		}
		// we have a constant in a conjunction
		// we (1) either prune the child
		// or (2) replace the entire conjunction with a constant
		auto constant = ExpressionExecutor::EvaluateScalar(*child);
		if (constant.IsNull()) {
			continue;
		}
		auto b = BooleanValue::Get(constant);
		bool prune_child = false;
		bool constant_value = true;
		if (b) {
			// true
			if (is_and) {
				// true in and: prune child
				prune_child = true;
			} else {
				// true in OR: replace with TRUE
				constant_value = true;
			}
		} else {
			// false
			if (is_and) {
				// false in AND: replace with FALSE
				constant_value = false;
			} else {
				// false in OR: prune child
				prune_child = true;
			}
		}
		if (prune_child) {
			expr.children.erase(expr.children.begin() + expr_idx);
			expr_idx--;
			continue;
		}
		*expr_ptr = make_unique<BoundConstantExpression>(Value::BOOLEAN(constant_value));
		return PropagateExpression(*expr_ptr);
	}
	if (expr.children.empty()) {
		// if there are no children left, replace the conjunction with TRUE (for AND) or FALSE (for OR)
		*expr_ptr = make_unique<BoundConstantExpression>(Value::BOOLEAN(is_and));
		return PropagateExpression(*expr_ptr);
	} else if (expr.children.size() == 1) {
		// if there is one child left, replace the conjunction with that one child
		*expr_ptr = move(expr.children[0]);
	}
	return nullptr;
}

} // namespace duckdb
