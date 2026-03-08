#include "duckdb/optimizer/outer_join_simplification.hpp"

#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

OuterJoinSimplification::OuterJoinSimplification() {
}

void OuterJoinSimplification::HandleExpression(const Expression &expr) {
	// TODO: could unwrap casts or basic arithmetic
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	null_filtered_columns.insert(colref.binding);
}

void OuterJoinSimplification::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
		case JoinType::INNER:
		case JoinType::SEMI: {
			// Derive bindings that cannot be NULL
			for (const auto &condition : join.conditions) {
				if (!condition.IsComparison()) {
					continue; // Non-comparison predicate, bail
				}
				if (condition.GetComparisonType() == ExpressionType::COMPARE_DISTINCT_FROM ||
				    condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
					continue; // Predicate does not filter NULL values
				}
				HandleExpression(condition.GetLHS());
				HandleExpression(condition.GetRHS());
			}
			VisitOperatorChildren(op);
			return;
		}
		case JoinType::LEFT:
		case JoinType::RIGHT:
		case JoinType::OUTER: {
			// Try to simplify joins
			bool preserves_null_extended_rows[2] = {
			    join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER,
			    join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER};
			for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
				for (const auto &binding : join.children[child_idx]->GetColumnBindings()) {
					if (null_filtered_columns.find(binding) != null_filtered_columns.end()) {
						// Rejecting NULLS in one child removes preservation of NULL extended rows for the other child
						preserves_null_extended_rows[1 - child_idx] = false;
						break;
					}
				}
			}

			if (!preserves_null_extended_rows[0] && !preserves_null_extended_rows[1]) {
				join.join_type = JoinType::INNER;
				VisitOperator(op); // Re-enter because we just created another (NULL-filtering!) INNER join
				return;
			}

			if (preserves_null_extended_rows[0] && !preserves_null_extended_rows[1]) {
				D_ASSERT(join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER);
				join.join_type = JoinType::LEFT;
			} else if (!preserves_null_extended_rows[0] && preserves_null_extended_rows[1]) {
				D_ASSERT(join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER);
				join.join_type = JoinType::RIGHT;
			} else {
				D_ASSERT(join.join_type == JoinType::OUTER);
				join.join_type = JoinType::OUTER;
			}

			VisitOperatorChildren(op);
			return;
		}
		default:
			// Passthrough not supported.
			break;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// Passthrough supported. Add input bindings
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t col_idx = 0; col_idx < projection.expressions.size(); col_idx++) {
			auto &expr = *projection.expressions[col_idx];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			const ColumnBinding binding(projection.table_index, col_idx);
			if (null_filtered_columns.find(binding) == null_filtered_columns.end()) {
				continue;
			}
			null_filtered_columns.insert(expr.Cast<BoundColumnRefExpression>().binding);
		}
		VisitOperatorChildren(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// Passthrough supported. Handle expressions that filter NULLs
		auto &filter = op.Cast<LogicalFilter>();
		filter.SplitPredicates(filter.expressions);
		for (const auto &expr : filter.expressions) {
			if (expr->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
			    expr->GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
				const auto &is_not_null = expr->Cast<BoundOperatorExpression>();
				HandleExpression(*is_not_null.children[0]);
			} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
				if (expr->GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM ||
				    expr->GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
					continue;
				}
				const auto &comparison = expr->Cast<BoundComparisonExpression>();
				HandleExpression(*comparison.left);
				HandleExpression(*comparison.right);
			}
		}
		VisitOperatorChildren(op);
		return;
	}
	default:
		// Passthrough not supported. TODO: could pass through more operators like LOGICAL_UNION
		break;
	}

	// Recurse with a new optimizer for each child
	for (auto &child : op.children) {
		OuterJoinSimplification outer_join_simplification;
		outer_join_simplification.VisitOperator(*child);
	}
}

} // namespace duckdb
