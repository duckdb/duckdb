#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"

namespace duckdb {
using namespace std;

// static bool HasNullFilters(vector<unique_ptr<Expression>> &expressions) {
// 	for(auto &expr: expressions) {
// 		if(expr->type == duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
// 			return true;
// 		}
// 	}
// 	return false;
// }

static bool IsComparisonFodable(unique_ptr<Expression> &expr) {
	if(expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = (BoundComparisonExpression &)*expr;
		// check if one of the sides is a scalar value
		return comparison.left->IsFoldable() || comparison.right->IsFoldable();
	}

	if(expr->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &comparison = (BoundBetweenExpression &)*expr;
		//! check if one of the sides is a scalar value
		return  comparison.lower->IsFoldable() || comparison.upper->IsFoldable();
	}
	//TODO it's missing to treat the case of duckdb::ExpressionType::CONJUNCTION_OR
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { return IsComparisonFodable(child); });
	return false;
}

static bool IsFilterFodable(vector<unique_ptr<Expression>> &expressions) {
	for(auto &expr: expressions) {
		if(!IsComparisonFodable(expr)) {
			return false;
		}
	}
	return true;
}

unique_ptr<LogicalOperator> FilterPullup::PullupFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);

	if(fork && IsFilterFodable(op->expressions)) {
		unique_ptr<LogicalOperator> child = move(op->children[0]);
		op->children.erase(op->children.begin());
		child = Rewrite(move(child));
		filters_pullup.push_back(move(op));
		return child;
	}
	op->children[0] = Rewrite(move(op->children[0]));
	return op;
}

} // namespace duckdb
