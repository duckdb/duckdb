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
	map<idx_t, idx_t> group_idxs;
	auto struct_input_exprs = GenerateAggregateArgs(new_bindings, window, generate_row_number, group_idxs);

	idx_t group_offset;

	auto limit = std::move(filter.expressions[0]->Cast<BoundComparisonExpression>().right);
	auto current_op = CreateAggregateOperator(window, std::move(limit), std::move(struct_input_exprs));
	group_offset = current_op->Cast<LogicalAggregate>().groups.size();
	current_op = CreateUnnestListOperator(std::move(current_op), generate_row_number);
	current_op = CreateUnnestStructOperator(std::move(current_op), generate_row_number, group_idxs);

	idx_t group_table_idx;
	idx_t aggregate_table_idx;

	if (current_op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		const auto &aggr = current_op->Cast<LogicalAggregate>();
		group_table_idx = aggr.group_index;
		aggregate_table_idx = aggr.aggregate_index;
	} else if (current_op->type == LogicalOperatorType::LOGICAL_UNNEST) {
		const auto &unnest = current_op->Cast<LogicalUnnest>();
		group_table_idx = unnest.children[0]->Cast<LogicalAggregate>().group_index;
		aggregate_table_idx = unnest.unnest_index;
	} else {
		D_ASSERT(current_op->type == LogicalOperatorType::LOGICAL_PROJECTION);
		group_table_idx = current_op->GetTableIndex()[0];
		aggregate_table_idx = group_table_idx;
	}

	UpdateBindings(window.window_index, group_table_idx, aggregate_table_idx, group_offset, group_idxs, old_bindings,
	               new_bindings, replacer);
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

	bool requires_arg = false;
	if (args.size() == 1) {
		requires_arg = true;
		aggregate_params.push_back(std::move(args[0]));
	} else if (args.size() > 1) {
		requires_arg = true;
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

	auto aggregate_expr = CreateAggregateExpression(std::move(aggregate_params), requires_arg, order_type);

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

unique_ptr<LogicalOperator>
TopNWindowElimination::CreateUnnestStructOperator(unique_ptr<LogicalOperator> op, const bool include_row_number,
                                                  const map<idx_t, idx_t> &group_idxs) const {
	LogicalType input_type;
	idx_t aggregate_table_idx;
	idx_t group_table_idx;

	if (op->type == LogicalOperatorType::LOGICAL_UNNEST) {
		auto &logical_unnest = op->Cast<LogicalUnnest>();
		const idx_t unnest_offset = logical_unnest.children[0]->types.size();
		input_type = logical_unnest.types[unnest_offset];
		aggregate_table_idx = logical_unnest.unnest_index;
		group_table_idx = logical_unnest.children[0]->Cast<LogicalAggregate>().group_index;
	} else {
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
		auto &logical_aggregate = op->Cast<LogicalAggregate>();
		const idx_t aggregate_column_idx = logical_aggregate.groups.size();
		input_type = logical_aggregate.types[aggregate_column_idx];
		aggregate_table_idx = logical_aggregate.aggregate_index;
		group_table_idx = logical_aggregate.group_index;
	}

	auto op_column_bindings = op->GetColumnBindings();

	vector<unique_ptr<Expression>> proj_exprs;
	// unnest_struct_exprs.reserve(group_idxs.size() + child_types.size() + 1);

	if (input_type.InternalType() != PhysicalType::STRUCT) {
		if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			// Project out unwanted group columns
			for (const auto &group_idx : group_idxs) {
				proj_exprs.push_back(
					make_uniq<BoundColumnRefExpression>(op->types[group_idx.second], op_column_bindings[group_idx.second]));
			}

			for (idx_t i = op->children[0]->types.size(); i < op->types.size(); i++) {
				proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(op->types[i], op_column_bindings[i]));
			}
		} else {
			for (const auto &group_idx : group_idxs) {
				proj_exprs.push_back(
					make_uniq<BoundColumnRefExpression>(op->types[group_idx.second], op_column_bindings[group_idx.second]));
			}

			// We do not have to unpack anything but we still might have to create a 1 constant as a row number
			for (idx_t i = op->Cast<LogicalAggregate>().groups.size(); i < op->types.size(); i++) {
				proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(op->types[i], op_column_bindings[i]));
			}
			proj_exprs.push_back(make_uniq<BoundConstantExpression>(static_cast<int64_t>(1)));
		}
	} else {
		for (idx_t i = 0; i < group_idxs.size(); i++) {
			proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(op->types[i], op_column_bindings[i]));
		}

		FunctionBinder function_binder(context);
		auto &catalog = Catalog::GetSystemCatalog(context);
		auto &struct_extract_entry =
		    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "struct_extract");
		const auto struct_extract_fun =
		    struct_extract_entry.functions.GetFunctionByArguments(context, {input_type, LogicalType::VARCHAR});

		const auto &child_types = StructType::GetChildTypes(input_type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			const auto &type = child_types[i];
			const auto &alias = type.first;
			vector<unique_ptr<Expression>> fun_args(2);
			fun_args[0] = make_uniq<BoundColumnRefExpression>(input_type, ColumnBinding(aggregate_table_idx, 0));
			fun_args[1] = make_uniq<BoundConstantExpression>(alias);
			auto bound_function = function_binder.BindScalarFunction(struct_extract_fun, std::move(fun_args));
			bound_function->alias = alias;
			proj_exprs.push_back(std::move(bound_function));
		}

		if (include_row_number) {
			// If aggregate (i.e., limit 1): constant, if unnest: expect there to be a second column
			if (op->type == LogicalOperatorType::LOGICAL_UNNEST) {
				const idx_t unnest_offset = op->children[0]->types.size();
				D_ASSERT(op->types.size() == unnest_offset + 2); // Row number should have been generated previously
				proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(op->types[unnest_offset + 1],
				                                                         ColumnBinding {aggregate_table_idx, 1}));
			} else {
				proj_exprs.push_back(make_uniq<BoundConstantExpression>(static_cast<int64_t>(1)));
			}
		}
	}

	auto logical_projection =
	    make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(proj_exprs));
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

vector<unique_ptr<Expression>> TopNWindowElimination::GenerateAggregateArgs(const vector<ColumnBinding> &bindings,
                                                                            const LogicalWindow &window,
                                                                            bool &generate_row_ids,
                                                                            map<idx_t, idx_t> &group_idxs) {
	vector<unique_ptr<Expression>> struct_pack_input_exprs;
	struct_pack_input_exprs.reserve(bindings.size());

	window.children[0]->ResolveOperatorTypes();
	const auto &window_child_types = window.children[0]->types;
	const auto window_child_bindings = window.children[0]->GetColumnBindings();
	const auto &window_expr = window.expressions[0]->Cast<BoundWindowExpression>();
	D_ASSERT(window_expr.orders[0].expression->type ==
	         ExpressionType::BOUND_COLUMN_REF); // FIXME: use BaseColumnReplacer
	const auto &aggregate_value_binding = window_expr.orders[0].expression->Cast<BoundColumnRefExpression>().binding;

	vector<ColumnBinding> group_bindings;
	for (const auto &expr : window_expr.partitions) {
		D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF); // FIXME
		const auto &column_ref = expr->Cast<BoundColumnRefExpression>();
		group_bindings.push_back(column_ref.binding);
	}

	for (idx_t i = 0; i < bindings.size(); i++) {
		auto &binding = bindings[i];
		auto group_binding = std::find(group_bindings.begin(), group_bindings.end(), binding);
		if (group_binding != group_bindings.end()) {
			const auto position_in_group = static_cast<idx_t>(group_binding - group_bindings.begin());
			group_idxs[i] = position_in_group;
			continue;
		}
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

	if (struct_pack_input_exprs.size() == 1) {
		// If we only project the aggregate value, we do not need arg_max/arg_min
		if (struct_pack_input_exprs[0]->Cast<BoundColumnRefExpression>().binding == aggregate_value_binding) {
			return {};
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

void TopNWindowElimination::UpdateBindings(const idx_t window_idx, const idx_t group_table_idx,
                                           const idx_t aggregate_table_idx, const idx_t group_offset,
                                           const map<idx_t, idx_t> &group_idxs,
                                           const vector<ColumnBinding> &old_bindings,
                                           vector<ColumnBinding> &new_bindings, ColumnBindingReplacer &replacer) {
	D_ASSERT(old_bindings.size() == new_bindings.size());
	D_ASSERT(replacer.replacement_bindings.empty());
	replacer.replacement_bindings.reserve(new_bindings.size());
	set<idx_t> row_id_binding_idxs;
	// Project the group columns
	idx_t struct_column_count = 0;
	for (auto group_idx : group_idxs) {
		new_bindings[group_idx.first].table_index = group_table_idx;
		new_bindings[group_idx.first].column_index = struct_column_count++;
		replacer.replacement_bindings.emplace_back(old_bindings[group_idx.first], new_bindings[group_idx.first]);
	}

	// Project the args/value
	// If this is a projection, all indexes are the same, so we start with group count as an offset
	struct_column_count = group_table_idx == aggregate_table_idx ? group_idxs.size() : 0;
	for (idx_t i = 0; i < new_bindings.size(); i++) {
		auto &binding = new_bindings[i];
		if (group_idxs.find(i) != group_idxs.end()) {
			continue;
		}
		if (binding.table_index == window_idx) {
			row_id_binding_idxs.insert(i);
		} else {
			binding.column_index = struct_column_count++;
			binding.table_index = aggregate_table_idx;
			replacer.replacement_bindings.emplace_back(old_bindings[i], binding);
		}
	}

	// Project the row number
	for (const auto row_id_binding_idx : row_id_binding_idxs) {
		// Let all projections on row id point to the last output column
		auto &binding = new_bindings[row_id_binding_idx];
		binding.table_index = aggregate_table_idx;
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
