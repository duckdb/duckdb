#include "duckdb/optimizer/topn_window_elimination.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

TopNWindowElimination::TopNWindowElimination(ClientContext &context_p, Optimizer &optimizer)
    : context(context_p), optimizer(optimizer) {
}

bool TopNWindowElimination::CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context) {
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	const auto &projection = op.Cast<LogicalProjection>();
	unordered_set<idx_t> table_idxs;
	for (const auto &expr : projection.expressions) {
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			table_idxs.insert(expr->Cast<BoundColumnRefExpression>().binding.table_index);
		}
	}

	const auto *child = op.children[0].get();
	if (child->type != LogicalOperatorType::LOGICAL_FILTER) {
		return false;
	}
	const auto &filter = child->Cast<LogicalFilter>();
	if (filter.expressions.size() != 1) {
		return false;
	}

	if (filter.expressions[0]->type != ExpressionType::COMPARE_LESSTHANOREQUALTO) {
		return false;
	}

	const auto &filter_comparison = filter.expressions[0]->Cast<BoundComparisonExpression>();
	if (filter_comparison.right->type != ExpressionType::VALUE_CONSTANT) {
		return false;
	}

	const auto &filter_reference = filter_comparison.left->Cast<BoundColumnRefExpression>();
	idx_t filter_table_idx = filter_reference.binding.table_index;
	idx_t filter_column_idx = filter_reference.binding.column_index;

	child = filter.children[0].get();
	while (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		const auto &projection = child->Cast<LogicalProjection>();
		if (projection.table_index == filter_table_idx) {
			if (projection.expressions[filter_column_idx]->type != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}
			const auto &column_ref = projection.expressions[filter_column_idx]->Cast<BoundColumnRefExpression>();
			filter_table_idx = column_ref.binding.table_index;
			filter_column_idx = column_ref.binding.column_index;
		}
		child = child->children[0].get();
	}

	if (child->type != LogicalOperatorType::LOGICAL_WINDOW) {
		return false;
	}
	const auto &window = child->Cast<LogicalWindow>();
	if (window.window_index != filter_table_idx) {
		return false;
	}
	// TODO: Check if window function is actually row number, not rank or something else

	// TODO: For now, we only support window functions in which we do not need the row number
	if (table_idxs.find(window.window_index) != table_idxs.end()) {
		return false;
	}
	// We have found a grouped top-n window construct!
	return true;
}

// CreateAggregateOperator: vec<Expr> struct_pack_children, window_expr, limit -> uniq<logOp>
unique_ptr<LogicalOperator> TopNWindowElimination::CreateAggregateOperator(vector<unique_ptr<Expression>> children,
                                                                           LogicalWindow &window,
                                                                           unique_ptr<Expression> limit) const {
	auto struct_pack_fun = StructPackFun::GetFunction();
	FunctionBinder function_binder(context);
	auto struct_pack_expr = function_binder.BindScalarFunction(struct_pack_fun, std::move(children));

	// TODO: Do not assume that window has duplicate expressions
	auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();
	D_ASSERT(window_expr.orders.size() == 1);

	vector<unique_ptr<Expression>> fun_params;
	fun_params.reserve(3);
	fun_params.push_back(std::move(struct_pack_expr));
	fun_params.push_back(std::move(window_expr.orders[0].expression));
	fun_params.push_back(std::move(limit));

	auto max_fun = MaxFun::GetFunctions().GetFunctionByOffset(1);
	max_fun.name = "max_by";
	auto bound_agg_fun = function_binder.BindAggregateFunction(max_fun, std::move(fun_params));

	vector<unique_ptr<Expression>> select_list(1);
	select_list[0] = std::move(bound_agg_fun);

	auto aggregate = make_uniq<LogicalAggregate>(optimizer.binder.GenerateTableIndex(),
	                                             optimizer.binder.GenerateTableIndex(), std::move(select_list));
	aggregate->groups = std::move(window_expr.partitions);
	GroupingSet grouping_set;
	for (idx_t i = 0; i < aggregate->grouping_sets.size(); ++i) {
		grouping_set.insert(i);
	}
	aggregate->grouping_sets.push_back(std::move(grouping_set));
	return aggregate;
}

unique_ptr<LogicalOperator>
TopNWindowElimination::CreateUnnestListOperator(const child_list_t<LogicalType> &input_types,
                                                idx_t aggregate_idx) const {
	auto unnest = make_uniq<LogicalUnnest>(optimizer.binder.GenerateTableIndex());
	auto struct_type = LogicalType::STRUCT(input_types);
	auto unnest_expr = make_uniq<BoundUnnestExpression>(struct_type);

	unnest_expr->child =
	    make_uniq<BoundColumnRefExpression>(LogicalType::LIST(struct_type), ColumnBinding(aggregate_idx, 0));
	unnest->expressions.push_back(std::move(unnest_expr));

	return unnest;
}

unique_ptr<LogicalOperator>
TopNWindowElimination::CreateUnnestStructOperator(const child_list_t<LogicalType> &input_types,
                                                  idx_t unnest_list_idx) const {
	FunctionBinder function_binder(context);

	vector<unique_ptr<Expression>> unnest_struct_exprs;
	unnest_struct_exprs.reserve(input_types.size());
	const auto struct_extract_fun = StructExtractFun::GetFunctions().GetFunctionByOffset(0);

	for (const auto &type : input_types) {
		const auto &alias = type.first;
		const auto &logical_type = type.second;
		vector<unique_ptr<Expression>> fun_args(2);
		fun_args[0] = make_uniq<BoundColumnRefExpression>(logical_type, ColumnBinding(unnest_list_idx, 0));
		fun_args[1] = make_uniq<BoundConstantExpression>(alias);
		auto bound_function = function_binder.BindScalarFunction(struct_extract_fun, std::move(fun_args));
		unnest_struct_exprs.push_back(std::move(bound_function));
	}
	return make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(unnest_struct_exprs));
}

unique_ptr<LogicalOperator> TopNWindowElimination::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op, &context)) {
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);
		auto *projection = &op->Cast<LogicalProjection>();
		auto struct_pack_input_exprs = std::move(projection->expressions);

		op = std::move(projection->children[0]);

		auto &filter = op->Cast<LogicalFilter>();
		auto &filter_expr = filter.expressions[0]->Cast<BoundComparisonExpression>();

		// Cycle through child projections and update table index
		auto *child = filter.children[0].get();
		while (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			projection = &child->Cast<LogicalProjection>();
			for (auto &expr : struct_pack_input_exprs) {
				D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF);
				auto &column_ref = expr->Cast<BoundColumnRefExpression>();

				if (column_ref.binding.table_index == projection->table_index) {
					const idx_t col_idx = column_ref.binding.column_index;
					D_ASSERT(projection->expressions.size() > col_idx &&
					         projection->expressions[col_idx]->type == ExpressionType::BOUND_COLUMN_REF);
					const auto &other_column_ref = projection->expressions[col_idx]->Cast<BoundColumnRefExpression>();
					column_ref.binding.table_index = other_column_ref.binding.table_index;
					column_ref.binding.column_index = other_column_ref.binding.column_index;
				}
			}
			child = child->children[0].get();
		}

		D_ASSERT(child->type == LogicalOperatorType::LOGICAL_WINDOW);
		auto &window = child->Cast<LogicalWindow>();

		child_list_t<LogicalType> struct_info;
		struct_info.reserve(struct_pack_input_exprs.size());

		for (const auto &expr : struct_pack_input_exprs) {
			struct_info.emplace_back(expr->alias, expr->return_type);
		}

		// Create logical operators
		auto aggregate =
		    CreateAggregateOperator(std::move(struct_pack_input_exprs), window, std::move(filter_expr.right));
		const idx_t aggregate_idx = aggregate->Cast<LogicalAggregate>().aggregate_index;

		auto unnest_list = CreateUnnestListOperator(struct_info, aggregate_idx);
		const idx_t unnest_list_idx = unnest_list->Cast<LogicalUnnest>().unnest_index;

		auto unnest_struct = CreateUnnestStructOperator(struct_info, unnest_list_idx);

		aggregate->children.push_back(Optimize(std::move(child->children[0])));
		unnest_list->children.push_back(std::move(aggregate));
		unnest_struct->children.push_back(std::move(unnest_list));

		return unnest_struct;
	}

	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}

	return op;
}

} // namespace duckdb
