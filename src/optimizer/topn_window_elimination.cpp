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
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace {

idx_t GetGroupIdx(const unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return op->Cast<LogicalAggregate>().group_index;
	}
	return op->GetTableIndex()[0];
}

idx_t GetAggregateIdx(const unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return op->Cast<LogicalAggregate>().aggregate_index;
	}
	return op->GetTableIndex()[0];
}

LogicalType GetAggregateType(const unique_ptr<LogicalOperator> &op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_UNNEST: {
		const auto &logical_unnest = op->Cast<LogicalUnnest>();
		const idx_t unnest_offset = logical_unnest.children[0]->types.size();
		return logical_unnest.types[unnest_offset];
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		const auto &logical_aggregate = op->Cast<LogicalAggregate>();
		const idx_t aggregate_column_idx = logical_aggregate.groups.size();
		return logical_aggregate.types[aggregate_column_idx];
	}
	default: {
		throw InternalException("Unnest or aggregate expected to extract aggregate type.");
	}
	}
}

vector<LogicalType> ExtractReturnTypes(const vector<unique_ptr<Expression>> &exprs) {
	vector<LogicalType> types;
	types.reserve(exprs.size());
	for (const auto &expr : exprs) {
		types.push_back(expr->return_type);
	}
	return types;
}

bool BindingsReferenceRowNumber(const vector<ColumnBinding> &bindings, const LogicalWindow &window) {
	for (const auto &binding : bindings) {
		if (binding.table_index == window.window_index) {
			return true;
		}
	}
	return false;
}

} // namespace

TopNWindowElimination::TopNWindowElimination(ClientContext &context_p, Optimizer &optimizer,
                                             optional_ptr<column_binding_map_t<unique_ptr<BaseStatistics>>> stats_p)
    : context(context_p), optimizer(optimizer), stats(stats_p) {
}

unique_ptr<LogicalOperator> TopNWindowElimination::Optimize(unique_ptr<LogicalOperator> op) {
	auto &extension_manager = context.db->GetExtensionManager();
	if (!extension_manager.ExtensionIsLoaded("core_functions")) {
		return op;
	}

	ColumnBindingReplacer replacer;
	op = OptimizeInternal(std::move(op), replacer);
	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperator(*op);
	}
	return op;
}

unique_ptr<LogicalOperator> TopNWindowElimination::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                                    ColumnBindingReplacer &replacer) {
	if (!CanOptimize(*op)) {
		// Traverse through query plan to find grouped top-n pattern
		if (op->children.size() > 1) {
			// If an operator has multiple children, we do not want them to overwrite each other's stop operator.
			// Thus, first update only the column binding in op, then set op as the new stop operator.
			for (auto &child : op->children) {
				ColumnBindingReplacer r2;
				child = OptimizeInternal(std::move(child), r2);

				if (!r2.replacement_bindings.empty()) {
					r2.VisitOperator(*op);
					replacer.replacement_bindings.insert(replacer.replacement_bindings.end(),
					                                     r2.replacement_bindings.begin(),
					                                     r2.replacement_bindings.end());
					replacer.stop_operator = op;
				}
			}
		} else if (!op->children.empty()) {
			op->children[0] = OptimizeInternal(std::move(op->children[0]), replacer);
		}

		return op;
	}
	// We have made sure that this is an operator sequence of filter -> N optional projections -> window
	auto &filter = op->Cast<LogicalFilter>();
	auto *child = filter.children[0].get();

	// Get bindings and types from filter to use in top-most operator later
	const auto topmost_bindings = filter.GetColumnBindings();
	auto new_bindings = TraverseProjectionBindings(topmost_bindings, child);

	D_ASSERT(child->type == LogicalOperatorType::LOGICAL_WINDOW);
	auto &window = child->Cast<LogicalWindow>();
	const idx_t window_idx = window.window_index;

	// Map the input column offsets of the group columns to the output offset if there are projections on the group
	// We use an ordered map here because we need to iterate over them in order later
	map<idx_t, idx_t> group_projection_idxs;
	auto aggregate_payload = GenerateAggregatePayload(new_bindings, window, group_projection_idxs);
	const auto params = ExtractOptimizerParameters(window, filter, new_bindings, aggregate_payload);

	// Optimize window children
	window.children[0] = Optimize(std::move(window.children[0]));

	op = CreateAggregateOperator(window, std::move(aggregate_payload), params);
	op = TryCreateUnnestOperator(std::move(op), params);
	op = CreateProjectionOperator(std::move(op), params, group_projection_idxs);

	D_ASSERT(op->type != LogicalOperatorType::LOGICAL_UNNEST);

	UpdateTopmostBindings(window_idx, op, group_projection_idxs, topmost_bindings, new_bindings, replacer);
	replacer.stop_operator = op.get();

	return unique_ptr<LogicalOperator>(std::move(op));
}

unique_ptr<Expression>
TopNWindowElimination::CreateAggregateExpression(vector<unique_ptr<Expression>> aggregate_params,
                                                 const bool requires_arg,
                                                 const TopNWindowEliminationParameters &params) const {
	auto &catalog = Catalog::GetSystemCatalog(context);
	FunctionBinder function_binder(context);

	// If the value column can be null, we must use the nulls_last function to follow null ordering semantics
	const bool change_to_arg = !requires_arg && params.can_be_null && params.limit > 1;
	if (change_to_arg) {
		// Copy value as argument
		aggregate_params.insert(aggregate_params.begin() + 1, aggregate_params[0]->Copy());
	}

	D_ASSERT(params.order_type == OrderType::ASCENDING || params.order_type == OrderType::DESCENDING);
	string fun_name = requires_arg || change_to_arg ? "arg_" : "";
	fun_name += params.order_type == OrderType::ASCENDING ? "min" : "max";
	fun_name += params.can_be_null && (requires_arg || change_to_arg) ? "_nulls_last" : "";

	auto &fun_entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, fun_name);
	const auto fun = fun_entry.functions.GetFunctionByArguments(context, ExtractReturnTypes(aggregate_params));
	return function_binder.BindAggregateFunction(fun, std::move(aggregate_params));
}

unique_ptr<LogicalOperator>
TopNWindowElimination::CreateAggregateOperator(LogicalWindow &window, vector<unique_ptr<Expression>> args,
                                               const TopNWindowEliminationParameters &params) const {
	auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();
	D_ASSERT(window_expr.orders.size() == 1);

	vector<unique_ptr<Expression>> aggregate_params;
	aggregate_params.reserve(3);

	const bool use_arg = !args.empty();
	if (args.size() == 1) {
		aggregate_params.push_back(std::move(args[0]));
	} else if (args.size() > 1) {
		// For more than one arg, we must use struct pack
		auto &catalog = Catalog::GetSystemCatalog(context);
		FunctionBinder function_binder(context);
		auto &struct_pack_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "struct_pack");
		const auto struct_pack_fun =
		    struct_pack_entry.functions.GetFunctionByArguments(context, ExtractReturnTypes(args));
		auto struct_pack_expr = function_binder.BindScalarFunction(struct_pack_fun, std::move(args));
		aggregate_params.push_back(std::move(struct_pack_expr));
	}

	aggregate_params.push_back(std::move(window_expr.orders[0].expression));
	if (params.limit > 1) {
		aggregate_params.push_back(std::move(make_uniq<BoundConstantExpression>(Value::BIGINT(params.limit))));
	}

	auto aggregate_expr = CreateAggregateExpression(std::move(aggregate_params), use_arg, params);

	vector<unique_ptr<Expression>> select_list;
	select_list.push_back(std::move(aggregate_expr));

	auto aggregate = make_uniq<LogicalAggregate>(optimizer.binder.GenerateTableIndex(),
	                                             optimizer.binder.GenerateTableIndex(), std::move(select_list));
	aggregate->groupings_index = optimizer.binder.GenerateTableIndex();
	aggregate->groups = std::move(window_expr.partitions);
	aggregate->children.push_back(std::move(window.children[0]));
	aggregate->ResolveOperatorTypes();

	return unique_ptr<LogicalOperator>(std::move(aggregate));
}

unique_ptr<Expression>
TopNWindowElimination::CreateRowNumberGenerator(unique_ptr<Expression> aggregate_column_ref) const {
	// Create unnest(generate_series(1, array_length(column_ref, 1))) function to generate row ids
	FunctionBinder function_binder(context);
	auto &catalog = Catalog::GetSystemCatalog(context);

	// array_length
	auto &array_length_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "array_length");
	vector<unique_ptr<Expression>> array_length_exprs;
	array_length_exprs.push_back(std::move(aggregate_column_ref));
	array_length_exprs.push_back(make_uniq<BoundConstantExpression>(1));

	const auto array_length_fun = array_length_entry.functions.GetFunctionByArguments(
	    context, {array_length_exprs[0]->return_type, array_length_exprs[1]->return_type});
	auto bound_array_length_fun = function_binder.BindScalarFunction(array_length_fun, std::move(array_length_exprs));

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

	return unique_ptr<Expression>(std::move(unnest_row_number_expr));
}

unique_ptr<LogicalOperator>
TopNWindowElimination::TryCreateUnnestOperator(unique_ptr<LogicalOperator> op,
                                               const TopNWindowEliminationParameters &params) const {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);

	auto &logical_aggregate = op->Cast<LogicalAggregate>();
	const idx_t aggregate_column_idx = logical_aggregate.groups.size();
	LogicalType aggregate_type = logical_aggregate.types[aggregate_column_idx];

	if (params.limit <= 1) {
		// LIMIT 1 -> we do not need to unnest
		return std::move(op);
	}

	// Create unnest expression for aggregate args
	const auto aggregate_bindings = logical_aggregate.GetColumnBindings();
	auto aggregate_column_ref =
	    make_uniq<BoundColumnRefExpression>(aggregate_type, aggregate_bindings[aggregate_column_idx]);

	vector<unique_ptr<Expression>> unnest_exprs;

	auto unnest_aggregate = make_uniq<BoundUnnestExpression>(ListType::GetChildType(aggregate_type));
	unnest_aggregate->child = aggregate_column_ref->Copy();
	unnest_exprs.push_back(std::move(unnest_aggregate));

	if (params.include_row_number) {
		// Create row number expression
		unnest_exprs.push_back(CreateRowNumberGenerator(std::move(aggregate_column_ref)));
	}

	auto unnest = make_uniq<LogicalUnnest>(optimizer.binder.GenerateTableIndex());
	unnest->expressions = std::move(unnest_exprs);
	unnest->children.push_back(std::move(op));
	unnest->ResolveOperatorTypes();

	return unique_ptr<LogicalOperator>(std::move(unnest));
}

void TopNWindowElimination::AddStructExtractExprs(
    vector<unique_ptr<Expression>> &exprs, const LogicalType &struct_type,
    const unique_ptr<BoundColumnRefExpression> &aggregate_column_ref) const {
	FunctionBinder function_binder(context);
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &struct_extract_entry =
	    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "struct_extract");
	const auto struct_extract_fun =
	    struct_extract_entry.functions.GetFunctionByArguments(context, {struct_type, LogicalType::VARCHAR});

	const auto &child_types = StructType::GetChildTypes(struct_type);
	for (idx_t i = 0; i < child_types.size(); i++) {
		const auto &alias = child_types[i].first;

		vector<unique_ptr<Expression>> fun_args(2);
		fun_args[0] = aggregate_column_ref->Copy();
		fun_args[1] = make_uniq<BoundConstantExpression>(alias);

		auto bound_function = function_binder.BindScalarFunction(struct_extract_fun, std::move(fun_args));
		bound_function->alias = alias;
		exprs.push_back(std::move(bound_function));
	}
}

unique_ptr<LogicalOperator>
TopNWindowElimination::CreateProjectionOperator(unique_ptr<LogicalOperator> op,
                                                const TopNWindowEliminationParameters &params,
                                                const map<idx_t, idx_t> &group_idxs) const {
	const auto aggregate_type = GetAggregateType(op);
	const idx_t aggregate_table_idx = GetAggregateIdx(op);
	const auto op_column_bindings = op->GetColumnBindings();

	vector<unique_ptr<Expression>> proj_exprs;
	// Only project necessary group columns
	for (const auto &group_idx : group_idxs) {
		proj_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(op->types[group_idx.second], op_column_bindings[group_idx.second]));
	}

	auto aggregate_column_ref =
	    make_uniq<BoundColumnRefExpression>(aggregate_type, ColumnBinding(aggregate_table_idx, 0));

	if (params.payload_type == TopNPayloadType::STRUCT_PACK) {
		AddStructExtractExprs(proj_exprs, aggregate_type, aggregate_column_ref);
	} else {
		// No need for struct_unpack! Just reference the aggregate column
		proj_exprs.push_back(std::move(aggregate_column_ref));
	}

	if (params.include_row_number) {
		// If aggregate (i.e., limit 1): constant, if unnest: expect there to be a second column
		if (op->type == LogicalOperatorType::LOGICAL_UNNEST) {
			const idx_t row_number_offset = op->children[0]->types.size() + 1;
			D_ASSERT(op->types.size() == row_number_offset + 1); // Row number should have been generated previously
			proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(op->types[row_number_offset],
			                                                         op_column_bindings[row_number_offset]));
		} else {
			proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(1)));
		}
	}

	auto logical_projection =
	    make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(proj_exprs));
	logical_projection->children.push_back(std::move(op));
	logical_projection->ResolveOperatorTypes();

	return unique_ptr<LogicalOperator>(std::move(logical_projection));
}

bool TopNWindowElimination::CanOptimize(LogicalOperator &op) {
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

	auto &filter_comparison = filter.expressions[0]->Cast<BoundComparisonExpression>();
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

	if (filter_comparison.left->type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	VisitExpression(&filter_comparison.left);

	auto *child = filter.children[0].get();
	while (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = child->Cast<LogicalProjection>();
		if (column_references.size() != 1) {
			column_references.clear();
			return false;
		}

		const auto current_column_ref = column_references.begin()->first;
		column_references.clear();
		D_ASSERT(current_column_ref.table_index == projection.table_index);
		VisitExpression(&projection.expressions[current_column_ref.column_index]);

		child = child->children[0].get();
	}

	if (column_references.size() != 1) {
		column_references.clear();
		return false;
	}
	const auto filter_col_idx = column_references.begin()->first.table_index;
	column_references.clear();

	if (child->type != LogicalOperatorType::LOGICAL_WINDOW) {
		return false;
	}
	const auto &window = child->Cast<LogicalWindow>();
	if (window.window_index != filter_col_idx) {
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
	auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();

	if (window_expr.orders.size() != 1) {
		return false;
	}
	if (window_expr.orders[0].type != OrderType::DESCENDING && window_expr.orders[0].type != OrderType::ASCENDING) {
		return false;
	}
	if (window_expr.orders[0].null_order != OrderByNullType::NULLS_LAST) {
		return false;
	}

	// We have found a grouped top-n window construct!
	return true;
}

vector<unique_ptr<Expression>> TopNWindowElimination::GenerateAggregatePayload(const vector<ColumnBinding> &bindings,
                                                                               const LogicalWindow &window,
                                                                               map<idx_t, idx_t> &group_idxs) {
	vector<unique_ptr<Expression>> aggregate_args;
	aggregate_args.reserve(bindings.size());

	window.children[0]->ResolveOperatorTypes();
	const auto &window_child_types = window.children[0]->types;
	const auto window_child_bindings = window.children[0]->GetColumnBindings();
	auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();

	// Remember order of group columns to recreate that order in new bindings later
	column_binding_map_t<idx_t> group_bindings;
	for (idx_t i = 0; i < window_expr.partitions.size(); i++) {
		auto &expr = window_expr.partitions[i];
		VisitExpression(&expr);
		group_bindings[column_references.begin()->first] = i;
		column_references.clear();
	}

	for (idx_t i = 0; i < bindings.size(); i++) {
		const auto &binding = bindings[i];
		const auto group_binding = group_bindings.find(binding);
		if (group_binding != group_bindings.end()) {
			group_idxs[i] = group_binding->second;
			continue;
		}
		if (binding.table_index == window.window_index) {
			continue;
		}
		auto column_id = to_string(binding.column_index); // Use idx as struct pack/extract identifier
		auto column_type = window_child_types[binding.column_index];
		const auto &column_binding = window_child_bindings[binding.column_index];

		aggregate_args.push_back(make_uniq<BoundColumnRefExpression>(column_id, column_type, column_binding));
	}

	if (aggregate_args.size() == 1) {
		// If we only project the aggregate value itself, we do not need it as an arg
		VisitExpression(&window_expr.orders[0].expression);
		const auto aggregate_value_binding = column_references.begin()->first;
		column_references.clear();

		if (window_expr.orders[0].expression->type == ExpressionType::BOUND_COLUMN_REF &&
		    aggregate_args[0]->Cast<BoundColumnRefExpression>().binding == aggregate_value_binding) {
			return {};
		}
	}

	return aggregate_args;
}

vector<ColumnBinding> TopNWindowElimination::TraverseProjectionBindings(const std::vector<ColumnBinding> &old_bindings,
                                                                        LogicalOperator *&op) {
	auto new_bindings = old_bindings;

	// Traverse child projections to retrieve projections on window output
	while (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op->Cast<LogicalProjection>();

		for (idx_t i = 0; i < new_bindings.size(); i++) {
			auto &new_binding = new_bindings[i];
			D_ASSERT(new_binding.table_index == projection.table_index);
			VisitExpression(&projection.expressions[new_binding.column_index]);
			new_binding = column_references.begin()->first;
			column_references.clear();
		}
		op = op->children[0].get();
	}

	return new_bindings;
}

void TopNWindowElimination::UpdateTopmostBindings(const idx_t window_idx, const unique_ptr<LogicalOperator> &op,
                                                  const map<idx_t, idx_t> &group_idxs,
                                                  const vector<ColumnBinding> &topmost_bindings,
                                                  vector<ColumnBinding> &new_bindings,
                                                  ColumnBindingReplacer &replacer) {
	// The top-most operator's column order is [group][aggregate args][row number]. Now, set the new resulting bindings.
	D_ASSERT(topmost_bindings.size() == new_bindings.size());
	replacer.replacement_bindings.reserve(new_bindings.size());
	set<idx_t> row_id_binding_idxs;

	const idx_t group_table_idx = GetGroupIdx(op);
	const idx_t aggregate_table_idx = GetAggregateIdx(op);

	// Project the group columns
	idx_t current_column_idx = 0;
	for (auto group_idx : group_idxs) {
		const idx_t group_referencing_idx = group_idx.first;
		new_bindings[group_referencing_idx].table_index = group_table_idx;
		new_bindings[group_referencing_idx].column_index = group_idx.second;
		replacer.replacement_bindings.emplace_back(topmost_bindings[group_referencing_idx],
		                                           new_bindings[group_referencing_idx]);
		current_column_idx++;
	}

	if (group_table_idx != aggregate_table_idx) {
		// If the topmost operator is not a projection, the table indexes are different, and we start back from 0
		current_column_idx = 0;
	}

	// Project the args/value
	for (idx_t i = 0; i < new_bindings.size(); i++) {
		auto &binding = new_bindings[i];
		if (group_idxs.find(i) != group_idxs.end()) {
			continue;
		}
		if (binding.table_index == window_idx) {
			row_id_binding_idxs.insert(i);
			continue;
		}
		binding.column_index = current_column_idx++;
		binding.table_index = aggregate_table_idx;
		replacer.replacement_bindings.emplace_back(topmost_bindings[i], binding);
	}

	// Project the row number
	for (const auto row_id_binding_idx : row_id_binding_idxs) {
		// Let all projections on row id point to the last output column
		auto &binding = new_bindings[row_id_binding_idx];
		binding.table_index = aggregate_table_idx;
		binding.column_index = op->types.size() - 1;
		replacer.replacement_bindings.emplace_back(topmost_bindings[row_id_binding_idx], binding);
	}
}

TopNWindowEliminationParameters
TopNWindowElimination::ExtractOptimizerParameters(const LogicalWindow &window, const LogicalFilter &filter,
                                                  const vector<ColumnBinding> &bindings,
                                                  vector<unique_ptr<Expression>> &aggregate_payload) {
	TopNWindowEliminationParameters params;

	auto &limit_expr = filter.expressions[0]->Cast<BoundComparisonExpression>().right;
	params.limit = limit_expr->Cast<BoundConstantExpression>().value.GetValue<int64_t>();
	params.include_row_number = BindingsReferenceRowNumber(bindings, window);
	params.payload_type = aggregate_payload.size() > 1 ? TopNPayloadType::STRUCT_PACK : TopNPayloadType::SINGLE_COLUMN;
	auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();
	params.order_type = window_expr.orders[0].type;

	VisitExpression(&window_expr.orders[0].expression);
	if (params.payload_type == TopNPayloadType::SINGLE_COLUMN && !aggregate_payload.empty()) {
		VisitExpression(&aggregate_payload[0]);
	}
	for (const auto &column_ref : column_references) {
		const auto &column_stats = stats->find(column_ref.first);
		if (column_stats == stats->end() || column_stats->second->CanHaveNull()) {
			params.can_be_null = true;
		}
	}
	column_references.clear();

	return params;
}

} // namespace duckdb
