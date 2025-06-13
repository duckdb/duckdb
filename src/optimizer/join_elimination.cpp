#include "duckdb/optimizer/join_elimination.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include <unordered_map>
#include <utility>

namespace duckdb {
unique_ptr<LogicalOperator> JoinElimination::OptimizeChildren(unique_ptr<LogicalOperator> op,
                                                              optional_ptr<LogicalOperator> parent) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		D_ASSERT(parent);
		auto &join = op->Cast<LogicalComparisonJoin>();
		// can check whether outer table has filter condition, if so then cannot eliminate
		if (join.filter_pushdown) {
			return std::move(op);
		}
		left_child = make_uniq<JoinElimination>();
		right_child = make_uniq<JoinElimination>();
		join.children[0] = left_child->Optimize(std::move(join.children[0]));
		join.children[1] = right_child->Optimize(std::move(join.children[1]));
		D_ASSERT(!join_parent);
		join_parent = parent;
		return std::move(op);
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op->Cast<LogicalDistinct>();
		if (distinct.distinct_type != DistinctType::DISTINCT) {
			break;
		}
		column_binding_set_t distinct_group;
		if (distinct.distinct_targets[0]->type != ExpressionType::BOUND_COLUMN_REF) {
			break;
		}
		idx_t table_idx = distinct.distinct_targets[0]->Cast<BoundColumnRefExpression>().binding.table_index;
		bool can_add = true;
		for (auto &target : distinct.distinct_targets) {
			if (distinct.distinct_targets[0]->type != ExpressionType::BOUND_COLUMN_REF) {
				can_add = false;
				break;
			}
			auto &col_ref = target->Cast<BoundColumnRefExpression>();
			distinct_group.insert(col_ref.binding);
			D_ASSERT(table_idx == col_ref.binding.table_index);
		}
		if (can_add) {
			distinct_groups[table_idx] = std::move(distinct_group);
		}
		break;
	}
	// case LogicalOperatorType::LOGICAL_UNNEST:
	// //FIXME: not sure window function could be eliminated, maybe harder
	// case LogicalOperatorType::LOGICAL_WINDOW: {
	// 	return std::move(op);
	// }
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op->Cast<LogicalAggregate>();
		if (aggr.grouping_sets.size() > 1) {
			break;
		}
		// only resolve group by columns for now
		column_binding_set_t distinct_group;
		idx_t table_idx = aggr.group_index;
		for (idx_t i = 0; i < aggr.groups.size(); i++) {
			distinct_group.insert(ColumnBinding(aggr.group_index, i));
		}
		if (!distinct_group.empty()) {
			ref_table_ids.insert(table_idx);
			distinct_groups[table_idx] = std::move(distinct_group);
		}
		VisitOperatorExpressions(*op);
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op->Cast<LogicalProjection>();
		VisitOperatorExpressions(*op);
		unordered_map<idx_t, vector<idx_t>> reference_records;
		// for select distinct * from table, first projection then distinct. distinct_groups has record projection table
		// id for select * from table group by col, first aggregate then projection. projection has aggregate table id.

		// before traverse children, first check whether any distinct group ref this projection
		auto it = distinct_groups.find(projection.table_index);
		bool could_add = true;
		if (it != distinct_groups.end()) {
			column_binding_set_t new_distinct_group;
			auto &expression = projection.expressions.get(it->second.begin()->column_index);
			if (expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				// if the expression is not a column ref, we cannot eliminate the join
				could_add = false;
				break;
			}
			idx_t ref_id = expression->Cast<BoundColumnRefExpression>().binding.table_index;
			for (auto &col : it->second) {
				auto &expression = projection.expressions.get(col.column_index);
				if (expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
					// if the expression is not a column ref, we cannot eliminate the join
					could_add = false;
					break;
				}
				auto &col_ref = expression->Cast<BoundColumnRefExpression>();
				D_ASSERT(ref_id == col_ref.binding.table_index);
				new_distinct_group.insert(col_ref.binding);
			}
			if (could_add) {
				distinct_groups[ref_id] = std::move(new_distinct_group);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op->Cast<LogicalGet>();
		if (!get.table_filters.filters.empty()) {
			inner_has_filter = true;
		}
		break;
	}
	default:
		break;
	}

	for (auto &child : op->children) {
		child = OptimizeChildren(std::move(child), op);
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op->Cast<LogicalProjection>();
		// after traversed children, here check whether any distinct group added in children
		unordered_map<idx_t, column_binding_set_t> ref_table_columns;
		for (idx_t idx = 0; idx < projection.expressions.size(); idx++) {
			auto &expression = projection.expressions.get(idx);
			if (expression->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = expression->Cast<BoundColumnRefExpression>();
				if (ref_table_columns.find(col_ref.binding.table_index) == ref_table_columns.end()) {
					ref_table_columns[col_ref.binding.table_index] = column_binding_set_t();
				}
				ref_table_columns[col_ref.binding.table_index].insert(ColumnBinding(projection.table_index, idx));
			}
		}
		for (auto &refs : ref_table_columns) {
			auto it = distinct_groups.find(refs.first);
			if (it != distinct_groups.end()) {
				auto columns_idx = refs.second;
				auto distinct_group = it->second;
				// lets's check whether the projection columns contains a whole distinct group carefully
				if (columns_idx.size() != distinct_group.size()) {
					continue;
				}
				distinct_groups[projection.table_index] = columns_idx;
			}
		}
		return std::move(op);
	}
	default:
		D_ASSERT(op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		VisitOperatorExpressions(*op);
		break;
	}
	return std::move(op);
}

unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	auto result = OptimizeChildren(std::move(op), nullptr);
	if (!join_parent) {
		return result;
	}
	for (auto &child : join_parent->children) {
		if (child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			child = TryEliminateJoin(std::move(child));
		}
	}
	return result;
}

unique_ptr<LogicalOperator> JoinElimination::TryEliminateJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(left_child != nullptr && right_child != nullptr);
	auto &join = op->Cast<LogicalComparisonJoin>();
	bool is_output_unique = false;
	switch (join.join_type) {
	case JoinType::LEFT: {
		inner_idx = 1;
		outer_idx = 0;
		break;
	case JoinType::SINGLE:
		inner_idx = 1;
		outer_idx = 0;
		is_output_unique = true;
		break;
	case JoinType::RIGHT:
		inner_idx = 0;
		outer_idx = 1;
		break;
	}
	default:
		return std::move(op);
	}
	auto &inner_child = inner_idx == 0 ? left_child : right_child;
	if (inner_child->inner_has_filter) {
		return std::move(op);
	}
	auto inner_bindings = join.children[inner_idx]->GetColumnBindings();
	// ensure join output columns only contains outer table columns
	for (auto &binding : inner_bindings) {
		if (ref_table_ids.find(binding.table_index) != ref_table_ids.end()) {
			return std::move(op);
		}
	}

	for (auto &distinct : left_child->distinct_groups) {
		distinct_groups[distinct.first] = distinct.second;
	}
	for (auto &distinct : right_child->distinct_groups) {
		distinct_groups[distinct.first] = distinct.second;
	}
	if (distinct_groups.empty()) {
		return std::move(op);
	}
	// 1. TODO: gurantee by primary/foreign key

	if (!is_output_unique) {
		is_output_unique = true;
		// 2. inner table join condition columns contains a whole distinct group
		vector<ColumnBinding> col_bindings;
		for (auto &condition : join.conditions) {
			if (condition.comparison != ExpressionType::COMPARE_EQUAL ||
			    condition.left->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
			    condition.right->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				is_output_unique = false;
				break;
			}
			auto inner_binding = inner_idx == 0 ? condition.left->Cast<BoundColumnRefExpression>().binding
			                                    : condition.right->Cast<BoundColumnRefExpression>().binding;
			col_bindings.push_back(inner_binding);
		}
		if (is_output_unique && !ContainDistinctGroup(col_bindings)) {
			is_output_unique = false;
		}
	}
	if (!is_output_unique) {
		// 3. join result columns in join condition contains a whole distinct group
		auto outer_bindings = join.children[outer_idx]->GetColumnBindings();
		if (ContainDistinctGroup(outer_bindings)) {
			is_output_unique = true;
		}
	}

	if (is_output_unique) {
		return std::move(op->children[outer_idx]);
	}
	return std::move(op);
}

bool JoinElimination::ContainDistinctGroup(vector<ColumnBinding> &column_bindings) {
	D_ASSERT(!column_bindings.empty());
	auto &column_binding = column_bindings[0];
	auto it = distinct_groups.find(column_binding.table_index);
	if (it == distinct_groups.end()) {
		return false;
	}
	unordered_set<idx_t> used_column_ids;
	for (auto &binding : column_bindings) {
		if (it->second.find(binding) == it->second.end()) {
			continue;
		}
		used_column_ids.emplace(binding.column_index);
	}
	return used_column_ids.size() == it->second.size();
}

unique_ptr<Expression> JoinElimination::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	ref_table_ids.insert(expr.binding.table_index);
	return nullptr;
}

} // namespace duckdb
