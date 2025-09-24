#include "duckdb/optimizer/topn_window_elimination.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {
TopNWindowElimination::TopNWindowElimination(ClientContext &context_p, Optimizer &optimizer)
    : context(context_p), optimizer(optimizer) {
}

unique_ptr<LogicalOperator> TopNWindowElimination::Optimize(unique_ptr<LogicalOperator> op) {
	bool update_table_idx;
	idx_t new_table_idx;
	return OptimizeInternal(std::move(op), update_table_idx, new_table_idx);
}

unique_ptr<LogicalOperator> TopNWindowElimination::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                    bool &update_table_idx, idx_t &new_table_idx) {
	if (CanOptimize(*op, &context)) {
		// We have made sure that this is an operator sequence of filter -> N optional projections -> window
		auto &filter = op->Cast<LogicalFilter>();
		auto &filter_expr = filter.expressions[0]->Cast<BoundComparisonExpression>();

		// Get bindings and types from filter to use in top-most operator later
		filter.ResolveOperatorTypes();
		auto target_bindings = filter.GetColumnBindings();
		auto target_types = std::move(filter.types);
		const idx_t last_op_idx = target_bindings[0].table_index;
		vector<string> target_names(target_bindings.size());

		// The filter parent may have projected out columns. Generate null constants as a padding s.t. the filter parent
		// accesses the correct output columns
		const auto padding_column_idxs = GeneratePaddingIdxs(target_bindings);

		// Cycle through child projections and update bindings to retrieve aggregate input
		bool has_intermediate_projections = false;
		auto *child = filter.children[0].get();

		while (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			has_intermediate_projections = true;
			auto &projection = child->Cast<LogicalProjection>();

			for (idx_t i = 0; i < target_bindings.size(); i++) {
				auto &binding = target_bindings[i];
				if (binding.table_index == projection.table_index) {
					D_ASSERT(projection.expressions[binding.column_index]->type == ExpressionType::BOUND_COLUMN_REF);
					auto &column_ref = projection.expressions[binding.column_index]->Cast<BoundColumnRefExpression>();
					binding.table_index = column_ref.binding.table_index;
					binding.column_index = column_ref.binding.column_index;
					if (target_names[i].empty()) {
						target_names[i] = column_ref.alias;
					}
				}
			}
			child = child->children[0].get();
		}

		D_ASSERT(child->type == LogicalOperatorType::LOGICAL_WINDOW);
		auto &window = child->Cast<LogicalWindow>();

		// Generate the column references for the struct_pack function that is used as the value for max_by
		vector<unique_ptr<Expression>> struct_pack_input_exprs;
		struct_pack_input_exprs.reserve(target_bindings.size());

		set<idx_t> row_number_idxs;
		bool include_row_number = false;

		if (!has_intermediate_projections) {
			// There is no projection between filter and window. Thus, we must change the filter parent's table index
			// in the column bindings of its expressions as the parent may reference output from the window operator
			// AND the window's child operator
			update_table_idx = true;
			new_table_idx = last_op_idx;
			// This also means that we cannot just steal the table index for our top-most operator as it may come from
			// the operator below the window
			idx_t new_child_idx = optimizer.binder.GenerateTableIndex();
			AssignChildNewTableIdx(window, new_child_idx);

			// Turn the filter bindings into inputs to the struct
			include_row_number =
			    GenerateStructPackInputExprs(window, target_bindings, target_names, target_types,
			                                 struct_pack_input_exprs, row_number_idxs, true, new_child_idx);
		} else {
			// Turn the filter bindings into inputs to the struct
			include_row_number = GenerateStructPackInputExprs(window, target_bindings, target_names, target_types,
			                                                  struct_pack_input_exprs, row_number_idxs);
		}

		child_list_t<LogicalType> unnest_info;
		unnest_info.reserve(struct_pack_input_exprs.size());

		for (const auto &expr : struct_pack_input_exprs) {
			unnest_info.emplace_back(expr->alias, expr->return_type);
		}

		// Create the logical operators
		auto aggregate = CreateAggregateOperator(std::move(struct_pack_input_exprs), window, filter_expr.right->Copy());
		const idx_t aggregate_idx = aggregate->Cast<LogicalAggregate>().aggregate_index;

		auto unnest_list =
		    CreateUnnestListOperator(unnest_info, aggregate_idx, include_row_number, std::move(filter_expr.right));
		const idx_t unnest_list_idx = unnest_list->Cast<LogicalUnnest>().unnest_index;

		auto unnest_struct =
		    CreateUnnestStructOperator(unnest_info, unnest_list_idx, last_op_idx, include_row_number, row_number_idxs);

		auto aggregate_child = Optimize(std::move(window.children[0]));

		aggregate->children.push_back(std::move(aggregate_child));
		unnest_list->children.push_back(std::move(aggregate));
		unnest_struct->children.push_back(std::move(unnest_list));

		// Add the padding columns
		auto &expressions = unnest_struct->expressions;
		for (auto padding_idx : padding_column_idxs) {
			expressions.insert(expressions.begin() + static_cast<int64_t>(padding_idx),
			                   make_uniq<BoundConstantExpression>(Value()));
		}

		return unnest_struct;
	}

	for (auto &child : op->children) {
		bool update_this_table_idx = false;
		idx_t table_idx = 0;
		child = OptimizeInternal(std::move(child), update_this_table_idx, table_idx);
		if (update_this_table_idx) {
			D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);
			UpdateTableIdxInExpressions(op->Cast<LogicalProjection>(), table_idx);
		}
	}

	return op;
}

// CreateAggregateOperator: vec<Expr> struct_pack_children, window_expr, limit -> uniq<logOp>
unique_ptr<LogicalOperator> TopNWindowElimination::CreateAggregateOperator(vector<unique_ptr<Expression>> children,
                                                                           LogicalWindow &window,
                                                                           unique_ptr<Expression> limit) const {
	auto struct_pack_fun = StructPackFun::GetFunction();
	FunctionBinder function_binder(context);
	auto struct_pack_expr = function_binder.BindScalarFunction(struct_pack_fun, std::move(children));

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

unique_ptr<LogicalOperator> TopNWindowElimination::CreateUnnestStructOperator(
    const child_list_t<LogicalType> &input_types, const idx_t unnest_list_idx, const idx_t table_idx,
    const bool include_row_number, const set<idx_t> &row_number_idxs) const {
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
		// TODO: Test if generate_series is still correct, if there are groups with less than k rows
		for (const auto row_number_idx : row_number_idxs) {
			auto row_number_reference =
			    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, ColumnBinding(unnest_list_idx, 1));
			unnest_struct_exprs.insert(unnest_struct_exprs.begin() + static_cast<int64_t>(row_number_idx),
			                           std::move(row_number_reference));
		}
	}

	return make_uniq<LogicalProjection>(table_idx, std::move(unnest_struct_exprs));
}

bool TopNWindowElimination::CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context) {
	if (op.type != LogicalOperatorType::LOGICAL_FILTER) {
		return false;
	}

	const auto &filter = op.Cast<LogicalFilter>();
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
	auto &filter_value = filter_comparison.right->Cast<BoundConstantExpression>();
	if (filter_value.value.type() != LogicalType::BIGINT) {
		return false;
	}
	if (filter_value.value.GetValue<int64_t>() <= 1) {
		return false;
	}

	const auto &filter_reference = filter_comparison.left->Cast<BoundColumnRefExpression>();
	idx_t filter_table_idx = filter_reference.binding.table_index;
	idx_t filter_column_idx = filter_reference.binding.column_index;

	auto *child = filter.children[0].get();
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
	if (window.expressions.size() != 1) {
		for (idx_t i = 1; i < window.expressions.size(); ++i) {
			if (!window.expressions[i]->Equals(*window.expressions[0])) {
				return false;
			}
		}
	}
	if (window.expressions[0]->type != ExpressionType::WINDOW_ROW_NUMBER) {
		return false;
	}
	const auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();
	if (window_expr.orders.size() != 1) {
		return false;
	}

	// We have found a grouped top-n window construct!
	return true;
}

void TopNWindowElimination::UpdateTableIdxInExpressions(LogicalProjection &projection, idx_t table_idx) {
	for (idx_t i = 0; i < projection.expressions.size(); i++) {
		auto &expr = projection.expressions[i];
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &column_ref = expr->Cast<BoundColumnRefExpression>();
			if (column_ref.binding.table_index != table_idx) {
				// This column references output from the window operator, which is in the i-th column of the child op
				column_ref.binding.table_index = table_idx;
				column_ref.binding.column_index = i;
			}
		}
	}
}

vector<idx_t> TopNWindowElimination::GeneratePaddingIdxs(const vector<ColumnBinding> &bindings) {
	set<idx_t> projected_column_idxs;
	for (auto &binding : bindings) {
		projected_column_idxs.insert(binding.column_index);
	}

	vector<idx_t> padding_column_idxs;
	padding_column_idxs.reserve(*projected_column_idxs.rbegin() - 1);
	auto projected_column_idx_it = projected_column_idxs.begin();

	for (idx_t i = 0; i < *projected_column_idxs.rbegin(); i++) {
		if (i < *projected_column_idx_it) {
			padding_column_idxs.push_back(i);
		} else {
			++projected_column_idx_it;
		}
	}
	return padding_column_idxs;
}

void TopNWindowElimination::AssignChildNewTableIdx(LogicalWindow &window, const idx_t table_idx) {
	auto *window_child = window.children[0].get();

	D_ASSERT(window_child->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	         window_child->type == LogicalOperatorType::LOGICAL_GET);
	if (window_child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = window_child->Cast<LogicalProjection>();
		projection.table_index = table_idx;
	} else {
		auto &get = window_child->Cast<LogicalGet>();
		get.table_index = table_idx;
	}

	for (auto &expr : window.expressions) {
		auto &window_expr = expr->Cast<BoundWindowExpression>();
		for (auto &partition : window_expr.partitions) {
			auto &column_ref = partition->Cast<BoundColumnRefExpression>();
			column_ref.binding.table_index = table_idx;
		}
		for (auto &order : window_expr.orders) {
			auto &column_ref = order.expression->Cast<BoundColumnRefExpression>();
			column_ref.binding.table_index = table_idx;
		}
	}
}
bool TopNWindowElimination::GenerateStructPackInputExprs(
    const LogicalWindow &window, const vector<ColumnBinding> &bindings, const vector<string> &column_names,
    const vector<LogicalType> &types, vector<unique_ptr<Expression>> &struct_input_exprs, set<idx_t> &row_number_idxs,
    bool use_new_child_idx, idx_t new_child_idx) {
	bool include_row_number = false;
	for (size_t i = 0; i < bindings.size(); i++) {
		if (bindings[i].table_index == window.window_index) {
			// This is a projection on row_number. We generate these numbers later in the unnest operator.
			include_row_number = true;
			row_number_idxs.insert(i);
			continue;
		}

		string column_name = column_names[i].empty() ? to_string(i) : column_names[i];
		if (use_new_child_idx) {
			struct_input_exprs.push_back(make_uniq<BoundColumnRefExpression>(
			    column_name, types[i], ColumnBinding {new_child_idx, bindings[i].column_index}));
		} else {
			struct_input_exprs.push_back(make_uniq<BoundColumnRefExpression>(column_name, types[i], bindings[i]));
		}
	}
	return include_row_number;
}

} // namespace duckdb
