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
	ColumnBindingReplacer replacer;
	return OptimizeInternal(std::move(op), replacer);
}

unique_ptr<LogicalOperator> TopNWindowElimination::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                    ColumnBindingReplacer &replacer) {
	if (!CanOptimize(*op, &context)) {
		// Traverse through query plan to find grouped top-n pattern
		for (auto &child : op->children) {
			child = OptimizeInternal(std::move(child), replacer);
			if (!replacer.replacement_bindings.empty()) {
				replacer.stop_operator = child;
				replacer.VisitOperator(*op);
				replacer = ColumnBindingReplacer();
			}
		}
		return op;
	}
	// We have made sure that this is an operator sequence of filter -> N optional projections -> window
	auto &filter = op->Cast<LogicalFilter>();
	auto *child = filter.children[0].get();

	// Get bindings and types from filter to use in top-most operator later
	const auto old_bindings = filter.GetColumnBindings();
	auto new_bindings = TraverseProjectionBindings(old_bindings, child);

	D_ASSERT(child->type == LogicalOperatorType::LOGICAL_WINDOW);
	auto &window = child->Cast<LogicalWindow>();

	// Generate the column references for the struct_pack function that is used as the value for max_by
	bool generate_row_number = false;
	auto struct_pack_input_exprs = GenerateStructPackExprs(new_bindings, window, generate_row_number);

	child_list_t<LogicalType> unnest_info;
	unnest_info.reserve(struct_pack_input_exprs.size());

	for (const auto &expr : struct_pack_input_exprs) {
		unnest_info.emplace_back(expr->alias, expr->return_type);
	}

	// Create the logical operators
	// Create aggregate with struct_pack
	auto &filter_constant = filter.expressions[0]->Cast<BoundComparisonExpression>().right;
	auto aggregate = CreateAggregateOperator(std::move(struct_pack_input_exprs), window, std::move(filter_constant));
	const idx_t aggregate_idx = aggregate->Cast<LogicalAggregate>().aggregate_index;

	// Create unnest list and generate row numbers
	auto unnest_list = CreateUnnestListOperator(unnest_info, aggregate_idx, generate_row_number);
	const idx_t unnest_list_idx = unnest_list->Cast<LogicalUnnest>().unnest_index;

	// Create unnest struct
	const idx_t unnest_struct_idx = optimizer.binder.GenerateTableIndex();
	auto unnest_struct =
	    CreateUnnestStructOperator(unnest_info, unnest_list_idx, unnest_struct_idx, generate_row_number);
	UpdateBindings(window.window_index, unnest_struct_idx, old_bindings, new_bindings, replacer);

	auto aggregate_child = Optimize(std::move(window.children[0]));

	aggregate->children.push_back(std::move(aggregate_child));
	unnest_list->children.push_back(std::move(aggregate));
	unnest_struct->children.push_back(std::move(unnest_list));

	return unnest_struct;
}

unique_ptr<LogicalAggregate> TopNWindowElimination::CreateAggregateOperator(vector<unique_ptr<Expression>> children,
                                                                            LogicalWindow &window,
                                                                            unique_ptr<Expression> limit) const {
	// Create arg_max(struct_pack(vals), args, limit)) function
	FunctionBinder function_binder(context);
	auto &catalog = Catalog::GetSystemCatalog(context);

	// struct_pack
	auto &struct_pack_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "struct_pack");
	const auto struct_pack_fun =
	    struct_pack_entry.functions.GetFunctionByArguments(context, ExtractReturnTypes(children));
	auto struct_pack_expr = function_binder.BindScalarFunction(struct_pack_fun, std::move(children));

	// arg_max
	auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();
	D_ASSERT(window_expr.orders.size() == 1);

	vector<unique_ptr<Expression>> arg_max_params;
	arg_max_params.reserve(3);

	arg_max_params.push_back(std::move(struct_pack_expr));
	arg_max_params.push_back(std::move(window_expr.orders[0].expression));
	arg_max_params.push_back(std::move(limit));

	// TODO: If limit is 1, use simple group by + max
	auto &arg_max_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "arg_max");
	const auto arg_max_fun =
	    arg_max_entry.functions.GetFunctionByArguments(context, ExtractReturnTypes(arg_max_params));
	auto arg_max_expr = function_binder.BindAggregateFunction(arg_max_fun, std::move(arg_max_params));

	// Create aggregate operator with arg_max expression and partitions from window function as groups
	vector<unique_ptr<Expression>> select_list(1);
	select_list[0] = std::move(arg_max_expr);

	auto aggregate = make_uniq<LogicalAggregate>(optimizer.binder.GenerateTableIndex(),
	                                             optimizer.binder.GenerateTableIndex(), std::move(select_list));
	aggregate->groupings_index = optimizer.binder.GenerateTableIndex();
	aggregate->groups = std::move(window_expr.partitions);

	return aggregate;
}

unique_ptr<LogicalUnnest> TopNWindowElimination::CreateUnnestListOperator(const child_list_t<LogicalType> &input_types,
                                                                          const idx_t aggregate_idx,
                                                                          const bool include_row_number) const {
	auto unnest = make_uniq<LogicalUnnest>(optimizer.binder.GenerateTableIndex());
	const auto struct_type = LogicalType::STRUCT(input_types);
	auto unnest_expr = make_uniq<BoundUnnestExpression>(struct_type);

	unnest_expr->child =
	    make_uniq<BoundColumnRefExpression>(LogicalType::LIST(struct_type), ColumnBinding(aggregate_idx, 0));
	unnest->expressions.push_back(std::move(unnest_expr));
	if (include_row_number) {
		// Create unnest(generate_series(1, array_length(column_ref, 1))) function to generate row ids
		FunctionBinder function_binder(context);

		// array_length
		auto &array_length_entry = Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(
		    context, DEFAULT_SCHEMA, "array_length");
		vector<unique_ptr<Expression>> array_length_exprs;
		array_length_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(LogicalType::LIST(struct_type), ColumnBinding(aggregate_idx, 0)));
		array_length_exprs.push_back(make_uniq<BoundConstantExpression>(1));

		const auto array_length_fun = array_length_entry.functions.GetFunctionByArguments(
		    context, {array_length_exprs[0]->return_type, array_length_exprs[1]->return_type});
		auto bound_array_length_fun =
		    function_binder.BindScalarFunction(array_length_fun, std::move(array_length_exprs));

		// generate_series
		auto &generate_series_entry = Catalog::GetSystemCatalog(context).GetEntry<ScalarFunctionCatalogEntry>(
		    context, DEFAULT_SCHEMA, "generate_series");

		vector<unique_ptr<Expression>> generate_series_exprs;
		generate_series_exprs.push_back(make_uniq<BoundConstantExpression>(1));
		generate_series_exprs.push_back(std::move(bound_array_length_fun));

		const auto generate_series_fun = generate_series_entry.functions.GetFunctionByArguments(
		    context, {generate_series_exprs[0]->return_type, generate_series_exprs[1]->return_type});
		auto bound_generate_series_fun =
		    function_binder.BindScalarFunction(generate_series_fun, std::move(generate_series_exprs));

		// unnest
		auto unnest_row_number_expr = make_uniq<BoundUnnestExpression>(LogicalType::BIGINT);
		unnest_row_number_expr->alias = "row_number";
		unnest_row_number_expr->child = std::move(bound_generate_series_fun);
		unnest->expressions.push_back(std::move(unnest_row_number_expr));
	}

	return unnest;
}

unique_ptr<LogicalProjection>
TopNWindowElimination::CreateUnnestStructOperator(const child_list_t<LogicalType> &input_types,
                                                  const idx_t unnest_list_idx, const idx_t table_idx,
                                                  const bool include_row_number) const {
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
		unnest_struct_exprs.push_back(std::move(row_number_reference));
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
	if (filter_value.value.GetValue<int64_t>() < 1) {
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
	if (window_expr.orders[0].type != OrderType::DESCENDING) {
		// TODO: Support ascending order with min_by
		return false;
	}

	// We have found a grouped top-n window construct!
	return true;
}

vector<unique_ptr<Expression>> TopNWindowElimination::GenerateStructPackExprs(const vector<ColumnBinding> &bindings,
                                                                              const LogicalWindow &window,
                                                                              bool &generate_row_ids) {
	vector<unique_ptr<Expression>> struct_pack_input_exprs;
	struct_pack_input_exprs.reserve(bindings.size());

	window.children[0]->ResolveOperatorTypes();
	const auto &window_child_types = window.children[0]->types;
	const auto window_child_bindings = window.children[0]->GetColumnBindings();

	for (idx_t i = 0; i < bindings.size(); i++) {
		auto &binding = bindings[i];
		if (binding.table_index == window.window_index) {
			generate_row_ids = true;
		} else {
			auto column_id = to_string(binding.column_index);
			auto column_type = window_child_types[binding.column_index];
			const auto &column_binding = window_child_bindings[binding.column_index];

			struct_pack_input_exprs.push_back(
			    make_uniq<BoundColumnRefExpression>(column_id, column_type, column_binding));
		}
	}

	return struct_pack_input_exprs;
}

vector<ColumnBinding> TopNWindowElimination::TraverseProjectionBindings(const std::vector<ColumnBinding> &old_bindings,
                                                                        LogicalOperator *&op) {
	auto new_bindings = old_bindings;

	// Traverse child projections to retrieve projections on window output
	while (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op->Cast<LogicalProjection>();

		for (idx_t i = 0; i < old_bindings.size(); i++) {
			auto &new_binding = new_bindings[i];
			if (new_binding.table_index == projection.table_index) {
				D_ASSERT(projection.expressions[new_binding.column_index]->type == ExpressionType::BOUND_COLUMN_REF);
				auto &column_ref = projection.expressions[new_binding.column_index]->Cast<BoundColumnRefExpression>();
				new_binding.table_index = column_ref.binding.table_index;
				new_binding.column_index = column_ref.binding.column_index;
			}
		}
		op = op->children[0].get();
	}

	return new_bindings;
}

void TopNWindowElimination::UpdateBindings(const idx_t window_idx, const idx_t new_table_idx,
                                           const vector<ColumnBinding> &old_bindings,
                                           vector<ColumnBinding> &new_bindings, ColumnBindingReplacer &replacer) {
	D_ASSERT(old_bindings.size() == new_bindings.size());
	replacer.replacement_bindings.reserve(new_bindings.size());
	set<idx_t> row_id_binding_idxs;
	idx_t struct_column_count = 0;

	for (idx_t i = 0; i < new_bindings.size(); i++) {
		auto &binding = new_bindings[i];
		if (binding.table_index == window_idx) {
			row_id_binding_idxs.insert(i);
		} else {
			binding.column_index = struct_column_count++;
			binding.table_index = new_table_idx;
			replacer.replacement_bindings.emplace_back(old_bindings[i], binding);
		}
	}

	for (const auto row_id_binding_idx : row_id_binding_idxs) {
		// Let all projections on row id point to the last output column
		auto &binding = new_bindings[row_id_binding_idx];
		binding.table_index = new_table_idx;
		binding.column_index = struct_column_count;
		replacer.replacement_bindings.emplace_back(old_bindings[row_id_binding_idx], binding);
	}
}

vector<LogicalType> TopNWindowElimination::ExtractReturnTypes(const vector<unique_ptr<Expression>> &exprs) {
	vector<LogicalType> types;
	types.reserve(exprs.size());
	for (const auto &expr : exprs) {
		types.push_back(expr->return_type);
	}
	return types;
}

} // namespace duckdb
