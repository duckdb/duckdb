#include "duckdb/optimizer/count_window_elimination.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

class TableRebinder : public LogicalOperatorVisitor {
public:
	TableRebinder(Optimizer &optimizer) : optimizer(optimizer) {
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

CountWindowElimination::CountWindowElimination(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> CountWindowElimination::Optimize(unique_ptr<LogicalOperator> op) {
	ColumnBindingReplacer replacer;
	op = OptimizeInternal(std::move(op), replacer);
	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*op);
	}
	return op;
}

unique_ptr<LogicalOperator> CountWindowElimination::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                     ColumnBindingReplacer &replacer) {
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		if (filter.expressions.size() == 1 && filter.children.size() == 1 &&
		    filter.children[0]->type == LogicalOperatorType::LOGICAL_WINDOW) {
			auto &window = filter.children[0]->Cast<LogicalWindow>();

			// Check recursively
			window.children[0] = OptimizeInternal(std::move(window.children[0]), replacer);

			if (window.expressions.size() != 1) {
				return op;
			}
			if (window.expressions[0]->type != ExpressionType::WINDOW_AGGREGATE) {
				return op;
			}

			// We can only optimize if there is a single window function equality comparison
			// Check matches
			if (filter.expressions[0]->type != ExpressionType::COMPARE_EQUAL) {
				return op;
			}
			auto &comp = filter.expressions[0]->Cast<BoundComparisonExpression>();
			if (comp.left->type != ExpressionType::BOUND_COLUMN_REF) {
				return op;
			}
			auto &col_ref = comp.left->Cast<BoundColumnRefExpression>();
			auto t_idx = col_ref.binding.table_index;
			auto c_idx = col_ref.binding.column_index;
			auto w_idx = window.window_index;

			if (t_idx != w_idx || c_idx != 0) {
				return op;
			}

			// Check right side is constant 1
			if (comp.right->type != ExpressionType::VALUE_CONSTANT) {
				return op;
			}
			auto &const_expr = comp.right->Cast<BoundConstantExpression>();
			if (!const_expr.value.type().IsIntegral()) {
				return op;
			}
			if (const_expr.value.GetValue<int64_t>() != 1) {
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
			TableRebinder rebinder(optimizer);
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

			// Filter on aggregate: count = 1
			// Count is the first aggregate, so it's at agg_op->groups.size() in the types list
			// Bindings: Aggregates are at aggregate_index
			auto cnt_ref = make_uniq<BoundColumnRefExpression>(agg_op->types[agg_op->groups.size()],
			                                                   ColumnBinding(aggregate_index, 0));

			auto filter_expr =
			    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(cnt_ref),
			                                         make_uniq<BoundConstantExpression>(Value::BIGINT(1)));

			auto rhs_filter = make_uniq<LogicalFilter>();
			rhs_filter->expressions.push_back(std::move(filter_expr));
			rhs_filter->children.push_back(std::move(agg_op));
			rhs_filter->ResolveOperatorTypes();

			// Semi Join
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);

			for (size_t i = 0; i < w_expr.partitions.size(); ++i) {
				JoinCondition cond;
				cond.comparison = ExpressionType::COMPARE_EQUAL;
				cond.left = w_expr.partitions[i]->Copy();
				cond.right = make_uniq<BoundColumnRefExpression>(w_expr.partitions[i]->return_type,
				                                                 ColumnBinding(group_index, i));
				join->conditions.push_back(std::move(cond));
			}

			join->children.push_back(std::move(original_child));
			join->children.push_back(std::move(rhs_filter));
			join->ResolveOperatorTypes();

			// Create Constant 1
			auto dummy_index = optimizer.binder.GenerateTableIndex();
			auto dummy = make_uniq<LogicalDummyScan>(dummy_index);
			dummy->ResolveOperatorTypes();

			auto const_one = make_uniq<BoundConstantExpression>(Value::BIGINT(1));
			const_one->alias = "count_window_result";

			auto proj_index = optimizer.binder.GenerateTableIndex();
			vector<unique_ptr<Expression>> proj_expressions;
			proj_expressions.push_back(std::move(const_one));

			auto projection = make_uniq<LogicalProjection>(proj_index, std::move(proj_expressions));
			projection->children.push_back(std::move(dummy));
			projection->ResolveOperatorTypes();

			// Cross Product
			auto cross = make_uniq<LogicalCrossProduct>(std::move(join), std::move(projection));
			cross->ResolveOperatorTypes();

			// Replace Count binding
			// Old window column: (window.window_index, 0)
			// New constant column: (proj_index, 0)
			ColumnBinding old_binding(window.window_index, 0);
			ColumnBinding new_binding(proj_index, 0);

			replacer.replacement_bindings.emplace_back(old_binding, new_binding);

			// We do NOT need to replace other bindings because CrossProduct preserves left child bindings,
			// and Window (presumably) passed through input bindings without re-binding.

			return std::move(cross);
		}
	} else if (!op->children.empty()) {
		for (auto &child : op->children) {
			child = OptimizeInternal(std::move(child), replacer);
		}
	}
	return op;
}

} // namespace duckdb
