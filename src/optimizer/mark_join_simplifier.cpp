#include "duckdb/optimizer/mark_join_simplifier.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_nullability.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

MarkJoinSimplifier::MarkJoinSimplifier(Optimizer &optimizer) : optimizer(optimizer) {
}

static bool HasOnlyNullSafeComparisons(const LogicalComparisonJoin &join) {
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || (condition.GetComparisonType() != ExpressionType::COMPARE_DISTINCT_FROM &&
		                                  condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
			return false;
		}
	}
	return true;
}

static bool IsMarker(const Expression &expr, ColumnBinding marker) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	const auto &column = expr.Cast<BoundColumnRefExpression>();
	return column.Depth() == 0 && column.Binding() == marker;
}

static void SimplifyNullSafeSemiJoinConditions(ClientContext &context, LogicalComparisonJoin &join) {
	NotNullExpressionAnalyzer analyzer(context);
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		// A UNION's active member may be NULL even when the UNION itself is not.
		if (condition.GetLHS().GetReturnType().id() == LogicalTypeId::UNION) {
			continue;
		}
		if (analyzer.IsNotNull(*join.children[0], condition.GetLHS()) ||
		    analyzer.IsNotNull(*join.children[1], condition.GetRHS())) {
			condition.SetComparisonType(ExpressionType::COMPARE_EQUAL);
		}
	}
}

bool MarkJoinSimplifier::TrySimplify(LogicalFilter &filter, LogicalOperator &root) {
	if (filter.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = filter.children[0]->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK || !join.duplicate_eliminated_columns.empty()) {
		// Retain unresolved correlation and DELIM semantics (issue #22267).
		return false;
	}
	const ColumnBinding marker(join.mark_index, ProjectionIndex(0));
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &expr = *filter.expressions[i];
		auto target_type = JoinType::INVALID;
		if (IsMarker(expr, marker)) {
			target_type = JoinType::SEMI;
		} else if (expr.GetExpressionType() == ExpressionType::OPERATOR_NOT &&
		           IsMarker(*expr.Cast<BoundOperatorExpression>().GetChildren()[0], marker) &&
		           HasOnlyNullSafeComparisons(join)) {
			target_type = JoinType::ANTI;
		}
		if (target_type == JoinType::INVALID) {
			continue;
		}
		// Reordering the converted join can change which rows evaluate volatile expressions.
		if (filter.HasVolatileExpressions()) {
			return false;
		}
		for (auto &condition : join.conditions) {
			// MARK tuple inequality uses row equality, unlike ordinary nested inequality.
			if (condition.IsComparison() && condition.GetComparisonType() == ExpressionType::COMPARE_NOTEQUAL &&
			    condition.GetLHS().GetReturnType().id() == LogicalTypeId::TUPLE) {
				return false;
			}
		}
		join.ResolveOperatorTypes();
		const auto old_bindings = join.GetColumnBindings();
		vector<unique_ptr<Expression>> expressions;
		for (idx_t col_idx = 0; col_idx < old_bindings.size(); col_idx++) {
			if (old_bindings[col_idx] == marker) {
				expressions.push_back(
				    make_uniq<BoundConstantExpression>(Value::BOOLEAN(target_type == JoinType::SEMI)));
			} else {
				expressions.push_back(make_uniq<BoundColumnRefExpression>(join.types[col_idx], old_bindings[col_idx]));
			}
		}
		join.join_type = target_type;
		join.mark_types.clear();
		filter.expressions.erase_at(i);
		if (target_type == JoinType::SEMI) {
			SimplifyNullSafeSemiJoinConditions(optimizer.context, join);
		}
		auto projection = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(expressions));
		projection->children.push_back(std::move(filter.children[0]));
		CorrelatedColumnBindingReplacer replacer;
		replacer.AddReplacements(old_bindings, projection->GetColumnBindings());
		replacer.BuildIndex();
		replacer.stop_operator = projection.get();
		filter.children[0] = std::move(projection);
		replacer.VisitOperator(root);
		return true;
	}
	return false;
}

void MarkJoinSimplifier::Rewrite(unique_ptr<LogicalOperator> &op, LogicalOperator &root) {
	for (auto &child : op->children) {
		Rewrite(child, root);
	}
	if (op->type != LogicalOperatorType::LOGICAL_FILTER) {
		return;
	}
	auto &filter = op->Cast<LogicalFilter>();
	if (TrySimplify(filter, root) && filter.expressions.empty() && !filter.HasProjectionMap()) {
		op = std::move(filter.children[0]);
	}
}

void MarkJoinSimplifier::Optimize(unique_ptr<LogicalOperator> &plan) {
	Rewrite(plan, *plan);
}

} // namespace duckdb
