#include "duckdb/optimizer/window_self_join.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

class WindowSelfJoinTableRebinder : public LogicalOperatorVisitor {
public:
	explicit WindowSelfJoinTableRebinder(Optimizer &optimizer) : optimizer(optimizer) {
	}

	unordered_map<idx_t, idx_t> table_map;
	Optimizer &optimizer;

	void VisitOperator(LogicalOperator &op) override {
		// Rebind definitions
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			auto new_idx = optimizer.binder.GenerateTableIndex();
			table_map[get.table_index] = new_idx;
			get.table_index = new_idx;
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = op.Cast<LogicalProjection>();
			auto new_idx = optimizer.binder.GenerateTableIndex();
			table_map[proj.table_index] = new_idx;
			proj.table_index = new_idx;
		}
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &agg = op.Cast<LogicalAggregate>();
			auto new_agg_idx = optimizer.binder.GenerateTableIndex();
			auto new_grp_idx = optimizer.binder.GenerateTableIndex();
			table_map[agg.aggregate_index] = new_agg_idx;
			table_map[agg.group_index] = new_grp_idx;
			agg.aggregate_index = new_agg_idx;
			agg.group_index = new_grp_idx;
		}
		// TODO: Handle other operators defining tables if needed
		// But Get/Projection/Aggregate are most common in subplans.

		VisitOperatorChildren(op);
		VisitOperatorExpressions(op);
	}

	void VisitExpression(unique_ptr<Expression> *expression) override {
		auto &expr = *expression;
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &bound = expr->Cast<BoundColumnRefExpression>();
			if (table_map.count(bound.binding.table_index)) {
				bound.binding.table_index = table_map[bound.binding.table_index];
			}
		}
		VisitExpressionChildren(**expression);
	}
};

WindowSelfJoinOptimizer::WindowSelfJoinOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> WindowSelfJoinOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	ColumnBindingReplacer replacer;
	op = OptimizeInternal(std::move(op), replacer);
	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*op);
	}
	return op;
}

unique_ptr<LogicalOperator> WindowSelfJoinOptimizer::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                      ColumnBindingReplacer &replacer) {
	if (op->type == LogicalOperatorType::LOGICAL_WINDOW) {
		auto &window = op->Cast<LogicalWindow>();

		// Check recursively
		window.children[0] = OptimizeInternal(std::move(window.children[0]), replacer);

		if (window.expressions.size() != 1) {
			return op;
		}
		if (window.expressions[0]->type != ExpressionType::WINDOW_AGGREGATE) {
			return op;
		}

		auto &w_expr = window.expressions[0]->Cast<BoundWindowExpression>();
		if (w_expr.aggregate->name != "count" && w_expr.aggregate->name != "count_star") {
			return op;
		}
		if (!w_expr.orders.empty()) {
			return op;
		}
		if (w_expr.partitions.empty()) {
			return op;
		}

		// --- Transformation ---

		auto original_child = std::move(window.children[0]);
		auto copy_child = original_child->Copy(optimizer.context);

		// Rebind copy_child to avoid duplicate table indices
		WindowSelfJoinTableRebinder rebinder(optimizer);
		rebinder.VisitOperator(*copy_child);

		auto aggregate_index = optimizer.binder.GenerateTableIndex();
		auto group_index = optimizer.binder.GenerateTableIndex();

		vector<unique_ptr<Expression>> groups;
		vector<unique_ptr<Expression>> aggregates;

		// Create Aggregate Operator
		for (auto &part : w_expr.partitions) {
			auto part_copy = part->Copy();
			rebinder.VisitExpression(&part_copy); // Update bindings
			groups.push_back(std::move(part_copy));
		}

		auto count_func = *w_expr.aggregate;
		unique_ptr<FunctionData> bind_info;
		if (w_expr.bind_info) {
			bind_info = w_expr.bind_info->Copy();
		} else {
			bind_info = nullptr;
		}

		vector<unique_ptr<Expression>> children;
		for (auto &child : w_expr.children) {
			auto child_copy = child->Copy();
			rebinder.VisitExpression(&child_copy); // Update bindings
			children.push_back(std::move(child_copy));
		}

		auto aggr_type = w_expr.distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT;

		auto agg_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(children), nullptr,
		                                                    std::move(bind_info), aggr_type);

		aggregates.push_back(std::move(agg_expr));

		// args: group_index, aggregate_index, ...
		auto agg_op = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));

		agg_op->groups = std::move(groups);
		agg_op->children.push_back(std::move(copy_child));
		agg_op->ResolveOperatorTypes();

		if (agg_op->types.size() <= agg_op->groups.size()) {
			throw InternalException("LogicalAggregate types size mismatch");
		}

		// Inner Join on the partition keys
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);

		for (size_t i = 0; i < w_expr.partitions.size(); ++i) {
			JoinCondition cond;
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			cond.left = w_expr.partitions[i]->Copy();
			cond.right =
			    make_uniq<BoundColumnRefExpression>(w_expr.partitions[i]->return_type, ColumnBinding(group_index, i));
			join->conditions.push_back(std::move(cond));
		}

		join->children.push_back(std::move(original_child));
		join->children.push_back(std::move(agg_op));
		join->ResolveOperatorTypes();

		// Replace Count binding
		// Old window column: (window.window_index, 0)
		// New constant column: (aggregate_index, 0)
		ColumnBinding old_binding(window.window_index, 0);
		ColumnBinding new_binding(aggregate_index, 0);

		replacer.replacement_bindings.emplace_back(old_binding, new_binding);

		return std::move(join);
	} else if (!op->children.empty()) {
		for (auto &child : op->children) {
			child = OptimizeInternal(std::move(child), replacer);
		}
	}
	return op;
}

} // namespace duckdb
