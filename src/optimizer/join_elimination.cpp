#include "duckdb/optimizer/join_elimination.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include <utility>

namespace duckdb {
	unique_ptr<LogicalOperator> JoinElimination::OptimizeChildren(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op->Cast<LogicalDistinct>();
		if (distinct.distinct_type != DistinctType::DISTINCT) {
			break;
		}
		column_binding_set_t distinct_group;
		idx_t table_idx = distinct.distinct_targets[0]->Cast<BoundColumnRefExpression>().binding.table_index;
		for (auto &target : distinct.distinct_targets) {
			D_ASSERT(target->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF);
			auto &col_ref = target->Cast<BoundColumnRefExpression>();
			distinct_group.insert(col_ref.binding);
			D_ASSERT(table_idx == col_ref.binding.table_index);
		}
		distinct_groups[table_idx] = std::move(distinct_group);
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
			column_references.insert(ColumnBinding(aggr.group_index, i));
		}
		distinct_groups[table_idx] = std::move(distinct_group);
		VisitOperatorExpressions(*op);
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op->Cast<LogicalProjection>();
		VisitOperatorExpressions(*op);
		unordered_map<idx_t, vector<idx_t>> reference_records;
		// for select distinct * from table, first projection then distinct. distinct_groups has record projection table id
		// for select * from table group by col, first aggregate then projection. projection has aggregate table id.
		auto it = distinct_groups.find(projection.table_index);
		if (it != distinct_groups.end()) {
			column_binding_set_t new_distinct_group;
			auto &expression = projection.expressions.get(it->second.begin()->column_index);
			if (expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				// if the expression is not a column ref, we cannot eliminate the join
				break;
			}
			idx_t ref_id = expression->Cast<BoundColumnRefExpression>().binding.table_index;
			for (auto &col: it->second) {
				auto &expression = projection.expressions.get(col.column_index);
				if (expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
					break;
				}
				auto &col_ref = expression->Cast<BoundColumnRefExpression>();
				D_ASSERT(ref_id == col_ref.binding.table_index);
				new_distinct_group.insert(col_ref.binding);
			}
			distinct_groups[ref_id] = std::move(new_distinct_group);
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
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		return TryEliminateJoin(std::move(op));
	}
	default:
		VisitOperatorExpressions(*op);
		break;
	}

	for (auto &child : op->children) {
		child = OptimizeChildren(std::move(child));
	}
	return std::move(op);
}

unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	return OptimizeChildren(std::move(op));
}

unique_ptr<LogicalOperator> JoinElimination::TryEliminateJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
	auto &join = op->Cast<LogicalComparisonJoin>();
	auto children_elimination = vector<JoinElimination> {JoinElimination(), JoinElimination()};
	join.children[0] = children_elimination[0].Optimize(std::move(join.children[0]));
	join.children[1] = children_elimination[1].Optimize(std::move(join.children[1]));

	if (join.filter_pushdown) {
		return std::move(op);
	}
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
	if (children_elimination[inner_idx].inner_has_filter) {
		return std::move(op);
	}
	auto inner_bindings = join.children[inner_idx]->GetColumnBindings();
	for (auto &binding : inner_bindings) {
		if (column_references.find(binding) != column_references.end()) {
			return std::move(op);
		}
	}

	for (auto &elimination : children_elimination) {
		for (auto &distinct: elimination.distinct_groups) {
			distinct_groups[distinct.first]= distinct.second;
		}
	}
	// 1. TODO: gurantee by primary/foreign key

	if (!is_output_unique) {
		is_output_unique = true;
		// 2. inner table join condition columns contains a whole distinct group
		vector<ColumnBinding> col_bindings;
		for (auto &condition: join.conditions) {
			if (condition.comparison != ExpressionType::COMPARE_EQUAL ||
				condition.left->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
				condition.right->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				is_output_unique = false;
				break;
			}
			auto inner_binding = inner_idx ==0? condition.left->Cast<BoundColumnRefExpression>().binding:condition.right->Cast<BoundColumnRefExpression>().binding;
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
	auto it =distinct_groups.find(column_binding.table_index);
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
	column_references.insert(expr.binding);
	return nullptr;
}

} // namespace duckdb
