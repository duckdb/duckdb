#include "duckdb/optimizer/mark_join_simplifier.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_nullability.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

MarkJoinSimplifier::MarkJoinSimplifier(ClientContext &context) : context(context) {
}

static bool IsTwoValued(const Expression &expr) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return true;
	case ExpressionType::VALUE_CONSTANT:
		return !expr.Cast<BoundConstantExpression>().GetValue().IsNull();
	case ExpressionType::OPERATOR_NOT:
		return IsTwoValued(*expr.Cast<BoundOperatorExpression>().GetChildren()[0]);
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR:
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			if (!IsTwoValued(*child)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static bool MatchPredicateIsTwoValued(const LogicalComparisonJoin &join) {
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			if (!IsTwoValued(condition.GetJoinExpression())) {
				return false;
			}
		} else if (condition.GetComparisonType() != ExpressionType::COMPARE_DISTINCT_FROM &&
		           condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
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

static bool CanChangeChildBindings(const LogicalOperator &op, idx_t child_idx) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELETE:
		return true;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		return child_idx == 1;
	default:
		// Positional inputs and unsupported operator contracts retain their complete schema.
		return false;
	}
}

void MarkJoinSimplifier::CountBindings(LogicalOperator &op) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const auto &expression) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    **expression, [&](const BoundColumnRefExpression &column) { binding_uses[column.Binding()]++; });
	});
	for (idx_t child_idx = 0; child_idx < op.children.size(); child_idx++) {
		auto &child = *op.children[child_idx];
		auto projection_map = LogicalOperatorVisitor::GetProjectionMap(op, child_idx);
		if ((projection_map && !projection_map->empty()) || !CanChangeChildBindings(op, child_idx)) {
			for (auto &binding : child.GetColumnBindings()) {
				binding_uses[binding]++;
			}
		}
		CountBindings(child);
	}
}

bool MarkJoinSimplifier::TrySimplify(LogicalFilter &filter) {
	if (filter.HasProjectionMap() || filter.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = filter.children[0]->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK || !join.duplicate_eliminated_columns.empty()) {
		// Retain unresolved correlation and DELIM semantics (issue #22267).
		return false;
	}
	const ColumnBinding marker(join.mark_index, ProjectionIndex(0));
	if (binding_uses[marker] != 1) {
		return false;
	}
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &expr = *filter.expressions[i];
		auto target_type = JoinType::INVALID;
		if (IsMarker(expr, marker)) {
			target_type = JoinType::SEMI;
		} else if (expr.GetExpressionType() == ExpressionType::OPERATOR_NOT &&
		           IsMarker(*expr.Cast<BoundOperatorExpression>().GetChildren()[0], marker) &&
		           MatchPredicateIsTwoValued(join)) {
			target_type = JoinType::ANTI;
		}
		if (target_type == JoinType::INVALID) {
			continue;
		}
		for (auto &condition : join.conditions) {
			// MARK tuple inequality uses row equality, unlike ordinary nested inequality.
			if (condition.IsComparison() && condition.GetComparisonType() == ExpressionType::COMPARE_NOTEQUAL &&
			    condition.GetLHS().GetReturnType().id() == LogicalTypeId::TUPLE) {
				return false;
			}
		}
		join.join_type = target_type;
		join.mark_types.clear();
		filter.expressions.erase_at(i);
		if (target_type == JoinType::SEMI) {
			SimplifyNullSafeSemiJoinConditions(context, join);
		}
		return true;
	}
	return false;
}

void MarkJoinSimplifier::Rewrite(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		Rewrite(child);
	}
	if (op->type != LogicalOperatorType::LOGICAL_FILTER) {
		return;
	}
	auto &filter = op->Cast<LogicalFilter>();
	if (TrySimplify(filter) && filter.expressions.empty()) {
		op = std::move(filter.children[0]);
	}
}

void MarkJoinSimplifier::Optimize(unique_ptr<LogicalOperator> &plan) {
	binding_uses.clear();
	for (auto &binding : plan->GetColumnBindings()) {
		binding_uses[binding]++;
	}
	CountBindings(*plan);
	Rewrite(plan);
}

} // namespace duckdb
