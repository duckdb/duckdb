#include "duckdb/optimizer/outer_join_simplification.hpp"

#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

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
				if (condition.GetLHS().GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					auto &colref = condition.GetLHS().Cast<BoundColumnRefExpression>();
					null_filtered.insert(colref.binding);
				}
				if (condition.GetRHS().GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					auto &colref = condition.GetRHS().Cast<BoundColumnRefExpression>();
					null_filtered.insert(colref.binding);
				}
			}
			VisitOperatorChildren(op);
			return;
		}
		case JoinType::LEFT:
		case JoinType::RIGHT:
		case JoinType::OUTER: {
			// Check which sides of the join we can reduce
			bool can_reduce[2] = {join.join_type == JoinType::LEFT, join.join_type == JoinType::RIGHT};
			for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
				if (can_reduce[child_idx]) {
					continue;
				}
				for (const auto &binding : join.children[child_idx]->GetColumnBindings()) {
					if (null_filtered.find(binding) != null_filtered.end()) {
						can_reduce[child_idx] = true;
						break;
					}
				}
			}

			if (can_reduce[0] && can_reduce[1]) {
				join.join_type = JoinType::INNER;
				// Re-enter because we just created another INNER join
				VisitOperator(op);
				return;
			}

			if (can_reduce[0]) {
				join.join_type = JoinType::LEFT;
			} else if (can_reduce[1]) {
				join.join_type = JoinType::RIGHT;
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
			if (null_filtered.find(binding) == null_filtered.end()) {
				continue;
			}
			null_filtered.insert(binding);
		}
		VisitOperatorChildren(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
		// Passthrough supported. TODO: could add more bindings if the filter rejects non-NULL
		VisitOperatorChildren(op);
		return;
	default:
		// Passthrough not supported. TODO: could also pass through LOGICAL_UNION
		break;
	}

	// Recurse with a new optimizer for each child
	for (auto &child : op.children) {
		OuterJoinSimplification outer_join_simplification;
		outer_join_simplification.VisitOperator(*child);
	}
}

} // namespace duckdb
