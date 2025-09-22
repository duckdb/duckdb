#include "duckdb/optimizer/topn_window_elimination.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
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
	vector<LogicalType> arg_types;
	fun_params.reserve(3);
	arg_types.reserve(3);
	arg_types.push_back(struct_pack_expr->return_type);
	fun_params.push_back(std::move(struct_pack_expr));
	arg_types.push_back(window_expr.orders[0].expression->return_type);
	fun_params.push_back(std::move(window_expr.orders[0].expression));
	arg_types.push_back(limit->return_type);
	fun_params.push_back(std::move(limit));
	// TODO: If limit is 1, use simple group by + max
	auto &function =
	    Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "arg_max");
	auto arg_max_fun = function.functions.GetFunctionByArguments(context, arg_types);

	auto bound_agg_fun = function_binder.BindAggregateFunction(arg_max_fun, std::move(fun_params));

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
TopNWindowElimination::CreateUnnestListOperator(const child_list_t<LogicalType> &input_types, const idx_t aggregate_idx,
                                                const bool include_row_number,
                                                unique_ptr<Expression> limit_value) const {
	auto unnest = make_uniq<LogicalUnnest>(optimizer.binder.GenerateTableIndex());
	auto struct_type = LogicalType::STRUCT(input_types);
	auto unnest_expr = make_uniq<BoundUnnestExpression>(struct_type);

	unnest_expr->child =
	    make_uniq<BoundColumnRefExpression>(LogicalType::LIST(struct_type), ColumnBinding(aggregate_idx, 0));
	unnest->expressions.push_back(std::move(unnest_expr));
	if (include_row_number) {
		FunctionBinder function_binder(context);

		auto &func = Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA,
		                                                                                     "generate_series");

		vector<unique_ptr<Expression>> generate_series_exprs;
		generate_series_exprs.push_back(make_uniq<BoundConstantExpression>(1));
		generate_series_exprs.push_back(std::move(limit_value));
		auto generate_series_fun = func.functions.GetFunctionByArguments(
		    context, {generate_series_exprs[0]->return_type, generate_series_exprs[1]->return_type});
		auto bound_generate_series_fun =
		    function_binder.BindScalarFunction(generate_series_fun, std::move(generate_series_exprs));
		auto unnest_row_number_expr = make_uniq<BoundUnnestExpression>(LogicalType::BIGINT);
		// TODO: set alias
		unnest_row_number_expr->child = std::move(bound_generate_series_fun);
		unnest->expressions.push_back(std::move(unnest_row_number_expr));
	}

	return unnest;
}

unique_ptr<LogicalOperator>
TopNWindowElimination::CreateUnnestStructOperator(const child_list_t<LogicalType> &input_types,
                                                  const idx_t unnest_list_idx, const idx_t table_idx,
                                                  const bool include_row_number, const idx_t row_number_idx) const {
	FunctionBinder function_binder(context);

	vector<unique_ptr<Expression>> unnest_struct_exprs;
	unnest_struct_exprs.reserve(input_types.size());
	const auto struct_extract_fun = StructExtractFun::GetFunctions().GetFunctionByOffset(0);
	const auto input_struct_type = LogicalType::STRUCT(input_types);

	for (idx_t i = 0; i < input_types.size(); i++) {
		const auto &type = input_types[i];
		const auto &alias = type.first;
		vector<unique_ptr<Expression>> fun_args(2);
		fun_args[0] = make_uniq<BoundColumnRefExpression>(input_struct_type, ColumnBinding(unnest_list_idx, 0));
		fun_args[1] = make_uniq<BoundConstantExpression>(alias);
		auto bound_function = function_binder.BindScalarFunction(struct_extract_fun, std::move(fun_args));
		bound_function->alias = alias;
		unnest_struct_exprs.push_back(std::move(bound_function));
	}

	if (include_row_number) {
		auto row_number_reference =
		    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, ColumnBinding(unnest_list_idx, 1));
		unnest_struct_exprs.insert(unnest_struct_exprs.begin() + static_cast<int64_t>(row_number_idx),
		                           std::move(row_number_reference));
	}

	return make_uniq<LogicalProjection>(table_idx, std::move(unnest_struct_exprs));
}

unique_ptr<LogicalOperator> TopNWindowElimination::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op, &context)) {
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);
		auto *projection = &op->Cast<LogicalProjection>();
		auto topmost_exprs = std::move(projection->expressions);
		const idx_t topmost_projection_idx = projection->table_index;

		op = std::move(projection->children[0]);

		auto &filter = op->Cast<LogicalFilter>();
		auto &filter_expr = filter.expressions[0]->Cast<BoundComparisonExpression>();

		// Cycle through child projections and update table index
		vector<idx_t> constant_idxs;
		constant_idxs.reserve(topmost_exprs.size());

		vector<unique_ptr<Expression>> struct_pack_input_exprs;
		struct_pack_input_exprs.reserve(topmost_exprs.size());

		for (idx_t i = 0; i < topmost_exprs.size(); i++) {
			if (topmost_exprs[i]->type == ExpressionType::VALUE_CONSTANT) {
				constant_idxs.push_back(i);
				continue;
			}
			if (topmost_exprs[i]->alias.empty()) {
				// We need aliases to struct_pack the columns
				topmost_exprs[i]->alias = to_string(i);
			}
			struct_pack_input_exprs.push_back(std::move(topmost_exprs[i]));
		}

		auto *child = filter.children[0].get();

		while (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			projection = &child->Cast<LogicalProjection>();
			for (auto &expr : struct_pack_input_exprs) {
				D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF ||
				         expr->type == ExpressionType::VALUE_CONSTANT);
				if (expr->type == ExpressionType::VALUE_CONSTANT) {
					continue;
				}

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

		bool include_row_number = false;
		idx_t row_number_idx = 0;
		for (idx_t i = 0; i < struct_pack_input_exprs.size(); i++) {
			auto &expr = struct_pack_input_exprs[i];
			if (expr->Cast<BoundColumnRefExpression>().binding.table_index == window.window_index) {
				include_row_number = true;
				row_number_idx = i;
				struct_pack_input_exprs.erase_at(i);
			}
		}

		child_list_t<LogicalType> struct_info;
		struct_info.reserve(struct_pack_input_exprs.size());

		for (const auto &expr : struct_pack_input_exprs) {
			struct_info.emplace_back(expr->alias, expr->return_type);
		}

		// Create logical operators
		auto aggregate = CreateAggregateOperator(std::move(struct_pack_input_exprs), window, filter_expr.right->Copy());
		const idx_t aggregate_idx = aggregate->Cast<LogicalAggregate>().aggregate_index;

		auto unnest_list =
		    CreateUnnestListOperator(struct_info, aggregate_idx, include_row_number, std::move(filter_expr.right));
		const idx_t unnest_list_idx = unnest_list->Cast<LogicalUnnest>().unnest_index;

		auto unnest_struct = CreateUnnestStructOperator(struct_info, unnest_list_idx, topmost_projection_idx,
		                                                include_row_number, row_number_idx);
		for (auto constant_idx : constant_idxs) {
			unnest_struct->expressions.insert(unnest_struct->expressions.begin() + static_cast<int64_t>(constant_idx),
			                                  std::move(topmost_exprs[constant_idx]));
		}

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
