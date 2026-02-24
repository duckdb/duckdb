#include "duckdb/optimizer/window_self_join.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
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
	static bool CanRebind(const LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET:
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_FILTER:
			if (!op.children.empty()) {
				return CanRebind(*op.children[0]);
			}
			return true;
		default:
			break;
		}
		return false;
	}

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
		if (op.type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
			auto &get = op.Cast<LogicalExpressionGet>();
			auto new_idx = optimizer.binder.GenerateTableIndex();
			table_map[get.table_index] = new_idx;
			get.table_index = new_idx;
		}
		if (op.type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
			auto &dummy = op.Cast<LogicalDummyScan>();
			auto new_idx = optimizer.binder.GenerateTableIndex();
			table_map[dummy.table_index] = new_idx;
			dummy.table_index = new_idx;
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

	void TranslateOrders(BoundAggregateExpression &result, const vector<BoundOrderByNode> &orders_bys) {
		result.order_bys = make_uniq<BoundOrderModifier>();
		auto &orders = result.order_bys->orders;
		for (auto &order : orders_bys) {
			auto order_copy = order.Copy();
			VisitExpression(&order_copy.expression); // Update bindings
			orders.emplace_back(std::move(order_copy));
		}
	}

	unique_ptr<Expression> TranslateAggregate(const BoundWindowExpression &w_expr) {
		auto agg_func = *w_expr.aggregate;
		unique_ptr<FunctionData> bind_info;
		if (w_expr.bind_info) {
			bind_info = w_expr.bind_info->Copy();
		} else {
			bind_info = nullptr;
		}

		vector<unique_ptr<Expression>> children;
		for (auto &child : w_expr.children) {
			auto child_copy = child->Copy();
			VisitExpression(&child_copy); // Update bindings
			children.push_back(std::move(child_copy));
		}

		unique_ptr<Expression> filter;
		if (w_expr.filter_expr) {
			filter = w_expr.filter_expr->Copy();
			VisitExpression(&filter); // Update bindings
		}

		auto aggr_type = w_expr.distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT;

		auto result = make_uniq<BoundAggregateExpression>(std::move(agg_func), std::move(children), std::move(filter),
		                                                  std::move(bind_info), aggr_type);

		if (!w_expr.arg_orders.empty()) {
			TranslateOrders(*result, w_expr.arg_orders);
		} else if (!w_expr.orders.empty()) {
			//	If the frame was ordered, copy the frame ordering to the aggregate function
			TranslateOrders(*result, w_expr.orders);
		}

		return std::move(result);
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

bool WindowSelfJoinOptimizer::CanOptimize(const BoundWindowExpression &w_expr,
                                          const BoundWindowExpression &w_expr0) const {
	if (w_expr.type != ExpressionType::WINDOW_AGGREGATE) {
		return false;
	}
	//	We can only accept ORDER BY clauses if the frame is the entire partition
	//	In that case, we will have to move the ordering clauses into the aggregate.
	switch (w_expr.start) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!w_expr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}

	switch (w_expr.end) {
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!w_expr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}
	if (w_expr.partitions.empty()) {
		return false;
	}
	if (w_expr.exclude_clause != WindowExcludeMode::NO_OTHER) {
		return false;
	}
	if (!w_expr.PartitionsAreEquivalent(w_expr0)) {
		return false;
	}
	return true;
}

unique_ptr<LogicalOperator> WindowSelfJoinOptimizer::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                      ColumnBindingReplacer &replacer) {
	if (op->type == LogicalOperatorType::LOGICAL_WINDOW) {
		auto &window = op->Cast<LogicalWindow>();

		// Check recursively
		window.children[0] = OptimizeInternal(std::move(window.children[0]), replacer);

		if (!WindowSelfJoinTableRebinder::CanRebind(*window.children[0])) {
			return op;
		}

		auto &w_expr0 = window.expressions[0]->Cast<BoundWindowExpression>();
		for (auto &expr : window.expressions) {
			auto &w_expr = expr->Cast<BoundWindowExpression>();
			if (!CanOptimize(w_expr, w_expr0)) {
				return op;
			}
		}
		auto &partitions = w_expr0.partitions;

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
		for (auto &part : partitions) {
			auto part_copy = part->Copy();
			rebinder.VisitExpression(&part_copy); // Update bindings
			groups.push_back(std::move(part_copy));
		}

		for (auto &expr : window.expressions) {
			auto &w_expr = expr->Cast<BoundWindowExpression>();
			aggregates.emplace_back(rebinder.TranslateAggregate(w_expr));
		}

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
		for (size_t i = 0; i < partitions.size(); ++i) {
			auto left_expr = partitions[i]->Copy();
			auto right_expr =
			    make_uniq<BoundColumnRefExpression>(partitions[i]->return_type, ColumnBinding(group_index, i));
			join->conditions.push_back(
			    JoinCondition(std::move(left_expr), std::move(right_expr), ExpressionType::COMPARE_NOT_DISTINCT_FROM));
		}

		join->children.push_back(std::move(original_child));
		join->children.push_back(std::move(agg_op));
		join->ResolveOperatorTypes();

		// Replace aggregate bindings
		// Old window column: (window.window_index, x)
		// New constant column: (aggregate_index, x)
		for (idx_t column_index = 0; column_index < window.expressions.size(); ++column_index) {
			ColumnBinding old_binding(window.window_index, column_index);
			ColumnBinding new_binding(aggregate_index, column_index);
			replacer.replacement_bindings.emplace_back(old_binding, new_binding);
		}

		return std::move(join);
	} else if (!op->children.empty()) {
		for (auto &child : op->children) {
			child = OptimizeInternal(std::move(child), replacer);
		}
	}
	return op;
}

} // namespace duckdb
