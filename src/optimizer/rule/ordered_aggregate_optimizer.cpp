#include "duckdb/optimizer/rule/ordered_aggregate_optimizer.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

OrderedAggregateOptimizer::OrderedAggregateOptimizer(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// we match on an OR expression within a LogicalFilter node
	root = make_uniq<ExpressionMatcher>();
	root->expr_class = ExpressionClass::BOUND_AGGREGATE;
}

unique_ptr<Expression> OrderedAggregateOptimizer::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                        bool &changes_made, bool is_root) {
	auto &aggr = bindings[0].get().Cast<BoundAggregateExpression>();
	if (!aggr.order_bys) {
		// no ORDER BYs defined
		return nullptr;
	}
	if (aggr.function.order_dependent == AggregateOrderDependent::NOT_ORDER_DEPENDENT) {
		// not an order dependent aggregate but we have an ORDER BY clause - remove it
		aggr.order_bys.reset();
		changes_made = true;
		return nullptr;
	}

	auto &context = rewriter.context;
	// for each ORDER BY - check if it is actually necessary
	// expressions that are in the groups do not need to be ORDERED BY
	// `ORDER BY` on a group has no effect, because for each aggregate, the group is unique
	// similarly, we only need to ORDER BY each aggregate once
	expression_set_t seen_expressions;
	auto &groups = op.Cast<LogicalAggregate>().groups;
	for (auto &target : groups) {
		seen_expressions.insert(*target);
	}
	vector<BoundOrderByNode> new_order_nodes;
	for (auto &order_node : aggr.order_bys->orders) {
		if (seen_expressions.find(*order_node.expression) != seen_expressions.end()) {
			// we do not need to order by this node
			continue;
		}
		seen_expressions.insert(*order_node.expression);
		new_order_nodes.push_back(std::move(order_node));
	}
	if (new_order_nodes.empty()) {
		aggr.order_bys.reset();
		changes_made = true;
		return nullptr;
	}
	aggr.order_bys->orders = std::move(new_order_nodes);

	//	Rewrite first/last/arbitrary/any_value to use arg_xxx[_null] and create_sort_key
	const auto &aggr_name = aggr.function.name;
	string arg_xxx_name;
	if (aggr_name == "last") {
		arg_xxx_name = "arg_max_null";
	} else if (aggr_name == "first" || aggr_name == "arbitrary") {
		arg_xxx_name = "arg_min_null";
	} else if (aggr_name == "any_value") {
		arg_xxx_name = "arg_min";
	} else {
		return nullptr;
	}

	FunctionBinder binder(context);
	vector<unique_ptr<Expression>> sort_children;
	for (auto &order : aggr.order_bys->orders) {
		sort_children.emplace_back(std::move(order.expression));

		string modifier;
		modifier += (order.type == OrderType::ASCENDING) ? "ASC" : "DESC";
		modifier += " NULLS";
		modifier += (order.null_order == OrderByNullType::NULLS_FIRST) ? " FIRST" : " LAST";
		sort_children.emplace_back(make_uniq<BoundConstantExpression>(Value(modifier)));
	}
	aggr.order_bys.reset();

	ErrorData error;
	auto sort_key = binder.BindScalarFunction(DEFAULT_SCHEMA, "create_sort_key", std::move(sort_children), error);
	if (!sort_key) {
		error.Throw();
	}

	auto &children = aggr.children;
	children.emplace_back(std::move(sort_key));

	//  Look up the arg_xxx_name function in the catalog
	QueryErrorContext error_context;
	auto &func = Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, SYSTEM_CATALOG, DEFAULT_SCHEMA, arg_xxx_name,
	                                                              error_context);
	D_ASSERT(func.type == CatalogType::AGGREGATE_FUNCTION_ENTRY);

	// bind the aggregate
	vector<LogicalType> types;
	for (const auto &child : children) {
		types.emplace_back(child->return_type);
	}
	auto best_function = binder.BindFunction(func.name, func.functions, types, error);
	if (best_function == DConstants::INVALID_INDEX) {
		error.Throw();
	}
	// found a matching function!
	auto bound_function = func.functions.GetFunctionByOffset(best_function);
	return binder.BindAggregateFunction(bound_function, std::move(children), std::move(aggr.filter),
	                                    aggr.IsDistinct() ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT);

	return nullptr;
}

} // namespace duckdb
