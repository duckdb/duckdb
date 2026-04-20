#include "duckdb/optimizer/window_self_join.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"

namespace duckdb {

static unique_ptr<Expression> TranslateAggregate(const BoundWindowExpression &w_expr) {
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
		children.push_back(std::move(child_copy));
	}

	unique_ptr<Expression> filter;
	if (w_expr.filter_expr) {
		filter = w_expr.filter_expr->Copy();
	}

	auto aggr_type = w_expr.distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT;

	auto result = make_uniq<BoundAggregateExpression>(std::move(agg_func), std::move(children), std::move(filter),
	                                                  std::move(bind_info), aggr_type);

	if (!w_expr.arg_orders.empty()) {
		result->order_bys = make_uniq<BoundOrderModifier>();
		auto &orders = result->order_bys->orders;
		for (auto &order : w_expr.arg_orders) {
			auto order_copy = order.Copy();
			orders.emplace_back(std::move(order_copy));
		}
	}

	return std::move(result);
}

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
	if (!w_expr.orders.empty()) {
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

	//	Even with no ORDER BY, we can still have a non-degenerate frame if we have ROWS framing.
	switch (w_expr.start) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::CURRENT_ROW_GROUPS:
		break;
	default:
		return false;
	}

	switch (w_expr.end) {
	case WindowBoundary::UNBOUNDED_FOLLOWING:
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::CURRENT_ROW_GROUPS:
		break;
	default:
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

		auto &w_expr0 = window.expressions[0]->Cast<BoundWindowExpression>();
		for (auto &expr : window.expressions) {
			auto &w_expr = expr->Cast<BoundWindowExpression>();
			if (!CanOptimize(w_expr, w_expr0)) {
				return op;
			}
		}
		auto &partitions = w_expr0.partitions;

		// --- Transformation ---
		// try to copy the LHS
		unique_ptr<LogicalOperator> copy_child;
		// Reuse the LogicalOperatorDeepCopy from CTE inlining, as the copying requirements are similar (copying an
		// operator subtree and replacing table indices)
		LogicalOperatorDeepCopy deep_copy(optimizer.binder, nullptr);
		try {
			copy_child = deep_copy.DeepCopy(window.children[0]);
		} catch (NotImplementedException &ex) {
			// failed to copy the LHS - cannot run this optimizer
			return op;
		}

		// We statically know that the copy child has the same column bindings as the original child,
		// just with different table indices. So we can prepare a replacer with the correct binding
		// replacements to avoid having to resolve anything again later on.
		ColumnBindingReplacer local_replacer;
		auto old_bindings = window.children[0]->GetColumnBindings();
		auto new_bindings = copy_child->GetColumnBindings();
		vector<ReplacementBinding> binding_replacements;
		for (idx_t i = 0; i < old_bindings.size(); ++i) {
			binding_replacements.emplace_back(old_bindings[i], new_bindings[i]);
		}
		local_replacer.replacement_bindings = std::move(binding_replacements);

		auto original_child = std::move(window.children[0]);

		auto aggregate_index = optimizer.binder.GenerateTableIndex();
		auto group_index = optimizer.binder.GenerateTableIndex();

		vector<unique_ptr<Expression>> groups;
		vector<unique_ptr<Expression>> aggregates;

		// Create Aggregate Operator
		for (auto &part : partitions) {
			auto part_copy = part->Copy();
			groups.push_back(std::move(part_copy));
		}

		for (auto &expr : window.expressions) {
			auto &w_expr = expr->Cast<BoundWindowExpression>();
			aggregates.emplace_back(TranslateAggregate(w_expr));
		}

		// args: group_index, aggregate_index, ...
		auto agg_op = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));

		agg_op->groups = std::move(groups);
		agg_op->children.push_back(std::move(copy_child));
		local_replacer.VisitOperator(*agg_op);
		agg_op->ResolveOperatorTypes();

		if (agg_op->types.size() <= agg_op->groups.size()) {
			throw InternalException("LogicalAggregate types size mismatch");
		}

		// Inner Join on the partition keys
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);

		for (size_t i = 0; i < partitions.size(); ++i) {
			JoinCondition cond;
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			cond.left = partitions[i]->Copy();
			cond.right = make_uniq<BoundColumnRefExpression>(partitions[i]->return_type, ColumnBinding(group_index, i));
			join->conditions.push_back(std::move(cond));
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
