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
	op = OptimizeInternal(std::move(op), replacer);
	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*op);
	}
	return op;
}

unique_ptr<LogicalOperator> TopNWindowElimination::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                    ColumnBindingReplacer &replacer) {
	if (!CanOptimize(*op, &context)) {
		// Traverse through query plan to find grouped top-n pattern
		for (auto &child : op->children) {
			child = OptimizeInternal(std::move(child), replacer);
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

	bool generate_row_number;
	auto struct_input_exprs = GenerateStructPackExprs(new_bindings, window, generate_row_number);

	auto limit = std::move(filter.expressions[0]->Cast<BoundComparisonExpression>().right);
	auto current_op = CreateAggregateOperator(window, std::move(limit), std::move(struct_input_exprs));
	current_op = CreateUnnestListOperator(std::move(current_op), generate_row_number);
	current_op = CreateUnnestStructOperator(std::move(current_op), generate_row_number);

	auto table_idxs = current_op->GetTableIndex();
	idx_t top_table_idx = table_idxs.size() == 1 ? table_idxs[0] : table_idxs[1];

	UpdateBindings(window.window_index, top_table_idx, old_bindings, new_bindings, replacer);
	replacer.stop_operator = current_op.get();

	return unique_ptr<LogicalOperator>(std::move(current_op));
}

unique_ptr<Expression> TopNWindowElimination::CreateAggregateExpression(vector<unique_ptr<Expression>> aggregate_params,
                                                                        const bool requires_arg,
                                                                        const OrderType order_type) const {
	auto &catalog = Catalog::GetSystemCatalog(context);
	FunctionBinder function_binder(context);

	D_ASSERT(order_type == OrderType::ASCENDING || order_type == OrderType::DESCENDING);
	string fun_name = requires_arg ? "arg_" : "";
	fun_name += order_type == OrderType::ASCENDING ? "min" : "max";

	auto &fun_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, fun_name);
	const auto fun = fun_entry.functions.GetFunctionByArguments(context, ExtractReturnTypes(aggregate_params));
	return function_binder.BindAggregateFunction(fun, std::move(aggregate_params));
}

unique_ptr<LogicalOperator> TopNWindowElimination::CreateAggregateOperator(LogicalWindow &window,
                                                                           unique_ptr<Expression> limit,
                                                                           vector<unique_ptr<Expression>> args) const {
	auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();
	D_ASSERT(window_expr.orders.size() == 1);
	const auto order_type = window_expr.orders[0].type;

	vector<unique_ptr<Expression>> aggregate_params;
	aggregate_params.reserve(3);

	bool use_struct_pack = false;
	if (args.size() == 1) {
		aggregate_params.push_back(std::move(args[0]));
	} else if (args.size() > 1) {
		use_struct_pack = true;
		auto &catalog = Catalog::GetSystemCatalog(context);
		FunctionBinder function_binder(context);
		auto &struct_pack_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "struct_pack");
		const auto struct_pack_fun =
		    struct_pack_entry.functions.GetFunctionByArguments(context, ExtractReturnTypes(args));
		auto struct_pack_expr = function_binder.BindScalarFunction(struct_pack_fun, std::move(args));
		aggregate_params.push_back(std::move(struct_pack_expr));
	}

	aggregate_params.push_back(std::move(window_expr.orders[0].expression));
	auto limit_value = limit->Cast<BoundConstantExpression>().value;
	if (limit_value > 1) {
		aggregate_params.push_back(std::move(limit));
	}

	auto aggregate_expr = CreateAggregateExpression(std::move(aggregate_params), use_struct_pack, order_type);

	// Create aggregate operator with arg_max expression and partitions from window function as groups
	vector<unique_ptr<Expression>> select_list(1);
	select_list[0] = std::move(aggregate_expr);

	auto aggregate = make_uniq<LogicalAggregate>(optimizer.binder.GenerateTableIndex(),
	                                             optimizer.binder.GenerateTableIndex(), std::move(select_list));
	aggregate->groupings_index = optimizer.binder.GenerateTableIndex();
	aggregate->groups = std::move(window_expr.partitions);
	aggregate->children.push_back(std::move(window.children[0]));
	aggregate->ResolveOperatorTypes();

	return unique_ptr<LogicalOperator>(std::move(aggregate));
}

unique_ptr<LogicalOperator> TopNWindowElimination::CreateUnnestListOperator(unique_ptr<LogicalOperator> op,
                                                                            const bool include_row_number) const {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	auto &logical_aggregate = op->Cast<LogicalAggregate>();
	const idx_t aggregate_column_idx = logical_aggregate.groups.size();
	LogicalType aggregate_type = logical_aggregate.types[aggregate_column_idx];

	if (aggregate_type.InternalType() != PhysicalType::LIST) {
		return std::move(op);
	}

	auto unnest = make_uniq<LogicalUnnest>(optimizer.binder.GenerateTableIndex());
	auto unnest_expr = make_uniq<BoundUnnestExpression>(ListType::GetChildType(aggregate_type));

	const auto aggregate_bindings = logical_aggregate.GetColumnBindings();
	unnest_expr->child = make_uniq<BoundColumnRefExpression>(aggregate_type, aggregate_bindings[aggregate_column_idx]);
	unnest->expressions.push_back(std::move(unnest_expr));

	if (include_row_number) {
		// Create unnest(generate_series(1, array_length(column_ref, 1))) function to generate row ids
		FunctionBinder function_binder(context);
		auto &catalog = Catalog::GetSystemCatalog(context);

		// array_length
		auto &array_length_entry =
		    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "array_length");
		vector<unique_ptr<Expression>> array_length_exprs;
		array_length_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(aggregate_type, aggregate_bindings[aggregate_column_idx]));
		array_length_exprs.push_back(make_uniq<BoundConstantExpression>(1));

		const auto array_length_fun = array_length_entry.functions.GetFunctionByArguments(
		    context, {array_length_exprs[0]->return_type, array_length_exprs[1]->return_type});
		auto bound_array_length_fun =
		    function_binder.BindScalarFunction(array_length_fun, std::move(array_length_exprs));

		// generate_series
		auto &generate_series_entry =
		    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "generate_series");

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

	unnest->children.push_back(std::move(op));
	unnest->ResolveOperatorTypes();

	return unique_ptr<LogicalOperator>(std::move(unnest));
}

unique_ptr<LogicalOperator> TopNWindowElimination::CreateUnnestStructOperator(unique_ptr<LogicalOperator> op,
                                                                              const bool include_row_number) const {
	LogicalType input_type;
	idx_t input_table_idx;

	if (op->type == LogicalOperatorType::LOGICAL_UNNEST) {
		auto &logical_unnest = op->Cast<LogicalUnnest>();
		input_type = logical_unnest.types[0];
		input_table_idx = logical_unnest.unnest_index;
	} else {
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
		auto &logical_aggregate = op->Cast<LogicalAggregate>();
		const idx_t aggregate_column_idx = logical_aggregate.groups.size();
		input_type = logical_aggregate.types[aggregate_column_idx];
		input_table_idx = logical_aggregate.aggregate_index;
	}

	D_ASSERT(input_type.InternalType() == PhysicalType::STRUCT); // TODO: for now, otherwise exit

	FunctionBinder function_binder(context);
	const auto &child_types = StructType::GetChildTypes(input_type);

	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &struct_extract_entry =
	    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "struct_extract");
	const auto struct_extract_fun =
	    struct_extract_entry.functions.GetFunctionByArguments(context, {input_type, LogicalType::VARCHAR});

	vector<unique_ptr<Expression>> unnest_struct_exprs;
	unnest_struct_exprs.reserve(child_types.size());

	for (idx_t i = 0; i < child_types.size(); i++) {
		const auto &type = child_types[i];
		const auto &alias = type.first;
		vector<unique_ptr<Expression>> fun_args(2);
		fun_args[0] = make_uniq<BoundColumnRefExpression>(input_type, ColumnBinding(input_table_idx, 0));
		fun_args[1] = make_uniq<BoundConstantExpression>(alias);
		auto bound_function = function_binder.BindScalarFunction(struct_extract_fun, std::move(fun_args));
		bound_function->alias = alias;
		unnest_struct_exprs.push_back(std::move(bound_function));
	}

	if (include_row_number) {
		// If aggregate (i.e., limit 1): constant, if unnest: expect there to be a second column
		if (op->type == LogicalOperatorType::LOGICAL_UNNEST) {
			D_ASSERT(op->types.size() == 2); // Row number should have been generated previously
			unnest_struct_exprs.push_back(
			    make_uniq<BoundColumnRefExpression>(op->types[1], ColumnBinding {input_table_idx, 1}));
		} else {
			unnest_struct_exprs.push_back(make_uniq<BoundConstantExpression>(static_cast<int64_t>(1)));
		}
	}

	auto logical_projection =
	    make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(unnest_struct_exprs));
	logical_projection->children.push_back(std::move(op));
	logical_projection->ResolveOperatorTypes();

	return logical_projection;
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
	if (window_expr.orders[0].type != OrderType::DESCENDING && window_expr.orders[0].type != OrderType::ASCENDING) {
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
	D_ASSERT(replacer.replacement_bindings.empty());
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
