#include "duckdb/optimizer/join_elimination.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {
unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	if (!finish_collection) {
		// We need to collect the unique constraints set at first, and we only need to collect once.
		CollectUniqueConstraintsSet(*op);
		finish_collection = true;
		if (unique_constraints_set.empty()) {
			// There are no unique constraints set, so we can return directly.
			return op;
		}
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		// Only support the normal comparison join now.
		auto &join = op->Cast<LogicalComparisonJoin>();
		idx_t inner_child_idx, outer_child_idx;
		auto join_type = join.join_type;
		// We only need to handle the left outer join here,
		// because at this point in the plan, only left joins exist.
		if (IsLeftOuterJoin(join_type)) {
			inner_child_idx = 1;
			outer_child_idx = 0;
		} else {
			D_ASSERT(!IsRightOuterJoin(join_type));
			break;
		}

		// Check whether there are used columns from the parent nodes are came from the inner side.
		bool all_matched = true;
		auto &inner_child = join.children[inner_child_idx];
		const auto &inner_bindings = inner_child->GetColumnBindings();
		for (idx_t col_idx = 0; col_idx < inner_bindings.size(); col_idx++) {
			if (column_references.find(inner_bindings[col_idx]) != column_references.end()) {
				all_matched = false;
				break;
			}
		}
		if (!all_matched) {
			break;
		}

		auto &outer_child = join.children[outer_child_idx];
		const auto &outer_bindings = outer_child->GetColumnBindings();
		column_binding_set_t outer_bindings_set;
		for (auto &binding : outer_bindings) {
			outer_bindings_set.insert(binding);
		}
		column_binding_set_t inner_bindings_set;
		for (auto &binding : inner_bindings) {
			inner_bindings_set.insert(binding);
		}
		// Collect the join key from the inner side
		column_binding_set_t inner_keys_set;
		// We need to check whether the join conditions are valid or not.
		// At first, we make more strict check.
		bool join_conditions_valid = true;
		for (const auto &cond : join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL ||
			    cond.left->type != ExpressionType::BOUND_COLUMN_REF ||
			    cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
				join_conditions_valid = false;
				break;
			}
			ColumnBinding &left_column_binding = cond.left->Cast<BoundColumnRefExpression>().binding;
			ColumnBinding &right_column_binding = cond.right->Cast<BoundColumnRefExpression>().binding;
			if (inner_bindings_set.find(right_column_binding) != inner_bindings_set.end() &&
			    outer_bindings_set.find(left_column_binding) != outer_bindings_set.end()) {
				inner_keys_set.insert(right_column_binding);
			} else {
				join_conditions_valid = false;
				break;
			}
		}

		if (!join_conditions_valid || inner_keys_set.empty()) {
			break;
		}

		// Check whether the inner keys are unique or not.
		bool can_remove = false;
		for (auto &unique_constraint_set : unique_constraints_set) {
			bool all_covered = true;
			D_ASSERT(!unique_constraint_set.empty());
			// Only need to guarantee that the columns in the inner keys are the superset of one unique_constraint_set.
			for (const auto &column_binding : unique_constraint_set) {
				if (inner_keys_set.find(column_binding) == inner_keys_set.end()) {
					all_covered = false;
					break;
				}
			}
			if (all_covered) {
				can_remove = true;
				break;
			}
		}

		if (can_remove) {
			// The outer join can be removed.
			return Optimize(std::move(outer_child));
		}
		break;
	}
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperatorExpressions(*op);

	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}

	return op;
}

void JoinElimination::CollectUniqueConstraintsSet(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable().get();
		if (!table) {
			return;
		}
		auto storage_info = table->GetStorageInfo(context);
		for (const auto &index : storage_info.index_info) {
			// Collect the unique constraints information from the metadata.
			if (index.is_unique) {
				// The column index in the index information is not the same in the output of the get operator.
				// We need to do some adjust based on the column_ids in the get operator.
				size_t col_cnt = 0;
				column_binding_set_t unique_constraint_set;
				for (idx_t column_index : index.column_set) {
					for (idx_t idx = 0; idx < get.column_ids.size(); idx++) {
						if (column_index == get.column_ids[idx]) {
							unique_constraint_set.insert(ColumnBinding(get.table_index, idx));
							col_cnt++;
							break;
						}
					}
				}
				// If all of the columns in the unique key have been used, we can store it.
				if (col_cnt == index.column_set.size()) {
					unique_constraints_set.push_back(std::move(unique_constraint_set));
				}
			}
		}
		return;
	}
	default:
		// TODO: support another operators later.
		//  Like projection, aggregation can also generate the new unique constraints set.
		break;
	}
	for (auto &child : op.children) {
		CollectUniqueConstraintsSet(*child);
	}
	return;
}

unique_ptr<Expression> JoinElimination::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// Add a column reference to record which column has been used.
	column_references.insert(expr.binding);
	return nullptr;
}

} // namespace duckdb
