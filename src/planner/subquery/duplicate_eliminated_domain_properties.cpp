//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/duplicate_eliminated_domain_properties.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/subquery/duplicate_eliminated_domain_properties.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

static bool IsColumnEqualityPredicate(Expression &expr) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		break;
	default:
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &lhs = BoundComparisonExpression::Left(comparison);
	auto &rhs = BoundComparisonExpression::Right(comparison);
	if (lhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
	    rhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	return lhs.Cast<BoundColumnRefExpression>().Depth() == 0 && rhs.Cast<BoundColumnRefExpression>().Depth() == 0;
}

static bool IsNonSelectiveJoinPredicate(Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		bool all_children_non_selective = true;
		ExpressionIterator::EnumerateChildren(
		    expr, [&](Expression &child) { all_children_non_selective &= IsNonSelectiveJoinPredicate(child); });
		return all_children_non_selective;
	}
	return IsColumnEqualityPredicate(expr);
}

bool DuplicateEliminatedDomainProperties::HasSelection(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		for (const auto &entry : get.table_filters) {
			auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(),
			                                                     "DuplicateEliminatedDomainProperties::HasSelection");
			auto &expr = *filter.expr;
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
			    expr.GetExpressionType() != ExpressionType::OPERATOR_IS_NOT_NULL) {
				return true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			if (!IsNonSelectiveJoinPredicate(*expr)) {
				return true;
			}
		}
		break;
	}
	default:
		break;
	}

	for (auto &child : op.children) {
		if (HasSelection(*child)) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
