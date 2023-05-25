#include "duckdb/optimizer/join_elimination.hpp"

#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {
unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &comp_join = op->Cast<LogicalComparisonJoin>();
		idx_t inner_child_idx;
		auto join_type = comp_join.join_type;
		// We only handle the left outer join and right outer join now.
		if (IsLeftOuterJoin(join_type)) {
			inner_child_idx = 1;
		} else if (IsRightOuterJoin(join_type)) {
			inner_child_idx = 0;
		} else {
			break;
		}

		// Check whether there are used columns for the parent node are came from the inner side.
		bool all_matched = true;
		auto &inner_child = comp_join.children[inner_child_idx];
		const auto all_inner_bindings = inner_child->GetColumnBindings();
		for (idx_t col_idx = 0; col_idx < all_inner_bindings.size(); col_idx++) {
			if (column_references.find(all_inner_bindings[col_idx]) != column_references.end()) {
				all_matched = false;
				break;
			}
		}
		if (!all_matched) {
			break;
		}

		// Check whether all of the unique columns are came from the outer side.
		auto &outer_child = comp_join.children[1^inner_child_idx];
		column_binding_set_t all_outer_bindings;
		for (const auto &binding : outer_child->GetColumnBindings()) {
			all_outer_bindings.insert(binding);
		}
		for (auto &binding : unique_column_references) {
			if (all_outer_bindings.find(binding) == all_outer_bindings.end()) {
				all_matched = false;
				break;
			}
		}

		if (all_matched) {
			op = std::move(outer_child);
		}

		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op->Cast<LogicalAggregate>();
		for (idx_t col_idx = 0; col_idx < aggr.expressions.size(); col_idx++) {
			if (aggr.expressions[col_idx]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				continue;
			}
			auto &aggr_expr = aggr.expressions[col_idx]->Cast<BoundAggregateExpression>();
			// Collect the unique columns.
			bool has_unique_column = false;
			if (aggr_expr.IsDistinct()) {
				has_unique_column = true;
			} else {
				auto agg_expr_function_name =  aggr_expr.function.name;
				if (agg_expr_function_name == "max" || agg_expr_function_name == "min") {
					has_unique_column = true;
				}
			}

			if (has_unique_column) {
				for (auto &child : aggr_expr.children) {
					if (child->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
						unique_column_references.insert(child->Cast<BoundColumnRefExpression>().binding);
					}
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op->Cast<LogicalDistinct>();
		for (idx_t col_idx = 0; col_idx < distinct.distinct_targets.size(); col_idx++) {
			if (distinct.distinct_targets[col_idx]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			unique_column_references.insert(distinct.distinct_targets[col_idx]->Cast<BoundColumnRefExpression>().binding);
		}
		break;
	}
	default:
		break;
	}

	LogicalOperatorVisitor::VisitOperatorExpressions(*op);

	column_binding_set_t parent_column_references;
	column_binding_set_t parent_unique_column_references;
	if (op->children.size() > 1) {
		// If there are more than one children. We should copy the column reference from the father
		// and not let each child influence each other.
		parent_column_references = column_references;
		parent_unique_column_references = unique_column_references;
	}
	for (idx_t idx = 0; idx < op->children.size(); idx++) {
		auto &child = op->children[idx];
		if (idx > 0) {
			D_ASSERT(idx < 2);
			// If we can guarantee that the number of children of all operators are less equal than 2.
			// We can use 'std::move' to save the copy cost.
			column_references = std::move(parent_column_references);
			unique_column_references = std::move(parent_unique_column_references);
		}
		child = Optimize(std::move(child));
	}
	return op;
}

unique_ptr<Expression> JoinElimination::VisitReplace(BoundColumnRefExpression &expr,
                                                         unique_ptr<Expression> *expr_ptr) {
	// Add a column reference to record which column has been used.
	column_references.insert(expr.binding);
	return nullptr;
}

unique_ptr<Expression> JoinElimination::VisitReplace(BoundReferenceExpression &expr,
                                                         unique_ptr<Expression> *expr_ptr) {
	// BoundReferenceExpression should not be used here yet, they only belong in the physical plan
	throw InternalException("BoundReferenceExpression should not be used here yet!");
}

} // namespace duckdb
