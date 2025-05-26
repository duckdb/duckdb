#include "duckdb/optimizer/join_elimination.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/helper.hpp"
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
		for (auto &target : distinct.distinct_targets) {
			if (target->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = target->Cast<BoundColumnRefExpression>();
				distinct_column_references.insert(col_ref.binding);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNNEST:
	//FIXME: not sure window function could be eliminated, maybe harder
	case LogicalOperatorType::LOGICAL_WINDOW: {
		return std::move(op);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op->Cast<LogicalAggregate>();
		if (aggr.grouping_sets.size() > 1) {
			break;
		}
		//????
		for (idx_t i = 0; i < aggr.groups.size(); i++) {
			distinct_column_references.insert(ColumnBinding(aggr.group_index, i));
			column_references.insert(ColumnBinding(aggr.group_index, i));
		}
		VisitOperatorExpressions(*op);
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op->Cast<LogicalProjection>();
		for (idx_t idx = 0; idx < projection.expressions.size(); idx++) {
			auto &expression = projection.expressions[idx];
			if (expression->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = expression->Cast<BoundColumnRefExpression>();
				auto binding = ColumnBinding(projection.table_index, idx);
				if (distinct_column_references.find(binding) != distinct_column_references.end()) {
					distinct_column_references.insert(col_ref.binding);
				}
				column_references.insert(col_ref.binding);
				column_references.insert(binding);
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

bool JoinElimination::IsDistinctExpression(Expression &expr) {
	return false;
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
		distinct_column_references.insert(elimination.distinct_column_references.begin(),
			                                      elimination.distinct_column_references.end());
	}
	// 1. TODO: gurantee by primary/foreign key

	if (!is_output_unique) {
		is_output_unique = true;
		// 2. inner table join condition columns are distinct
		for (auto &condition: join.conditions) {
			if (condition.comparison != ExpressionType::COMPARE_EQUAL ||
				condition.left->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
				condition.right->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				is_output_unique = false;
				break;
			}
			auto inner_binding = inner_idx ==0? condition.left->Cast<BoundColumnRefExpression>().binding:condition.right->Cast<BoundColumnRefExpression>().binding;
			if (distinct_column_references.find(inner_binding) == distinct_column_references.end()) {
				is_output_unique = false;
				break;
			}
		}
	}
	if (!is_output_unique) {
		is_output_unique = true;
		// 3. join result columns in join condition are all distinct
		auto outer_bindings = join.children[outer_idx]->GetColumnBindings();
		for (auto &binding : outer_bindings) {
			if (distinct_column_references.find(binding) == distinct_column_references.end()) {
				is_output_unique = false;
				break;
			}
		}
	}

	if (is_output_unique) {
		return std::move(op->children[outer_idx]);
	}
	return std::move(op);
}

} // namespace duckdb
