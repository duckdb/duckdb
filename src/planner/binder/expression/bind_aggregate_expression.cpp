#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

static void InvertPercentileFractions(ClientContext &context, unique_ptr<ParsedExpression> &fractions) {
	D_ASSERT(fractions.get());
	D_ASSERT(fractions->expression_class == ExpressionClass::BOUND_EXPRESSION);
	auto &bound = (BoundExpression &)*fractions;

	if (!bound.expr->IsFoldable()) {
		return;
	}

	Value value = ExpressionExecutor::EvaluateScalar(context, *bound.expr);
	if (value.type().id() == LogicalTypeId::LIST) {
		vector<Value> values;
		for (const auto &element_val : ListValue::GetChildren(value)) {
			if (element_val.IsNull()) {
				values.push_back(element_val);
			} else {
				values.push_back(Value::DOUBLE(1 - element_val.GetValue<double>()));
			}
		}
		bound.expr = make_unique<BoundConstantExpression>(Value::LIST(values));
	} else if (value.IsNull()) {
		bound.expr = make_unique<BoundConstantExpression>(value);
	} else {
		bound.expr = make_unique<BoundConstantExpression>(Value::DOUBLE(1 - value.GetValue<double>()));
	}
}

BindResult SelectBinder::BindAggregate(FunctionExpression &aggr, AggregateFunctionCatalogEntry *func, idx_t depth) {
	// first bind the child of the aggregate expression (if any)
	this->bound_aggregate = true;
	unique_ptr<Expression> bound_filter;
	AggregateBinder aggregate_binder(binder, context);
	string error, filter_error;

	// Now we bind the filter (if any)
	if (aggr.filter) {
		aggregate_binder.BindChild(aggr.filter, 0, error);
	}

	// Handle ordered-set aggregates by moving the single ORDER BY expression to the front of the children.
	//	https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE
	bool ordered_set_agg = false;
	bool invert_fractions = false;
	if (aggr.order_bys && aggr.order_bys->orders.size() == 1) {
		const auto &func_name = aggr.function_name;
		ordered_set_agg = (func_name == "quantile_cont" || func_name == "quantile_disc" || func_name == "mode");

		if (ordered_set_agg) {
			auto &config = DBConfig::GetConfig(context);
			const auto &order = aggr.order_bys->orders[0];
			const auto sense =
			    (order.type == OrderType::ORDER_DEFAULT) ? config.options.default_order_type : order.type;
			invert_fractions = (sense == OrderType::DESCENDING);
		}
	}

	for (auto &child : aggr.children) {
		aggregate_binder.BindChild(child, 0, error);
		// We have to invert the fractions for PERCENTILE_XXXX DESC
		if (error.empty() && invert_fractions) {
			InvertPercentileFractions(context, child);
		}
	}

	// Bind the ORDER BYs, if any
	if (aggr.order_bys && !aggr.order_bys->orders.empty()) {
		for (auto &order : aggr.order_bys->orders) {
			aggregate_binder.BindChild(order.expression, 0, error);
		}
	}

	if (!error.empty()) {
		// failed to bind child
		if (aggregate_binder.HasBoundColumns()) {
			for (idx_t i = 0; i < aggr.children.size(); i++) {
				// however, we bound columns!
				// that means this aggregation belongs to this node
				// check if we have to resolve any errors by binding with parent binders
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.children[i]);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.children[i];
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			}
			if (aggr.filter) {
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.filter);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.filter;
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			}
			if (aggr.order_bys && !aggr.order_bys->orders.empty()) {
				for (auto &order : aggr.order_bys->orders) {
					bool success = aggregate_binder.BindCorrelatedColumns(order.expression);
					if (!success) {
						throw BinderException(error);
					}
					auto &bound_expr = (BoundExpression &)*order.expression;
					ExtractCorrelatedExpressions(binder, *bound_expr.expr);
				}
			}
		} else {
			// we didn't bind columns, try again in children
			return BindResult(error);
		}
	} else if (depth > 0 && !aggregate_binder.HasBoundColumns()) {
		return BindResult("Aggregate with only constant parameters has to be bound in the root subquery");
	}
	if (!filter_error.empty()) {
		return BindResult(filter_error);
	}

	if (aggr.filter) {
		auto &child = (BoundExpression &)*aggr.filter;
		bound_filter = BoundCastExpression::AddCastToType(context, move(child.expr), LogicalType::BOOLEAN);
	}

	// all children bound successfully
	// extract the children and types
	vector<LogicalType> types;
	vector<LogicalType> arguments;
	vector<unique_ptr<Expression>> children;

	if (ordered_set_agg) {
		for (auto &order : aggr.order_bys->orders) {
			auto &child = (BoundExpression &)*order.expression;
			types.push_back(child.expr->return_type);
			arguments.push_back(child.expr->return_type);
			children.push_back(move(child.expr));
		}
		aggr.order_bys->orders.clear();
	}

	for (idx_t i = 0; i < aggr.children.size(); i++) {
		auto &child = (BoundExpression &)*aggr.children[i];
		types.push_back(child.expr->return_type);
		arguments.push_back(child.expr->return_type);
		children.push_back(move(child.expr));
	}

	// bind the aggregate
	FunctionBinder function_binder(context);
	idx_t best_function = function_binder.BindFunction(func->name, func->functions, types, error);
	if (best_function == DConstants::INVALID_INDEX) {
		throw BinderException(binder.FormatError(aggr, error));
	}
	// found a matching function!
	auto bound_function = func->functions.GetFunctionByOffset(best_function);

	// Bind any sort columns, unless the aggregate is order-insensitive
	auto order_bys = make_unique<BoundOrderModifier>();
	if (!aggr.order_bys->orders.empty()) {
		auto &config = DBConfig::GetConfig(context);
		for (auto &order : aggr.order_bys->orders) {
			auto &order_expr = (BoundExpression &)*order.expression;
			const auto sense =
			    (order.type == OrderType::ORDER_DEFAULT) ? config.options.default_order_type : order.type;
			const auto null_order = (order.null_order == OrderByNullType::ORDER_DEFAULT)
			                            ? config.options.default_null_order
			                            : order.null_order;
			order_bys->orders.emplace_back(BoundOrderByNode(sense, null_order, move(order_expr.expr)));
		}
	}

	auto aggregate = function_binder.BindAggregateFunction(
	    bound_function, move(children), move(bound_filter),
	    aggr.distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT, move(order_bys));
	if (aggr.export_state) {
		aggregate = ExportAggregateFunction::Bind(move(aggregate));
	}

	// check for all the aggregates if this aggregate already exists
	idx_t aggr_index;
	auto entry = node.aggregate_map.find(aggregate.get());
	if (entry == node.aggregate_map.end()) {
		// new aggregate: insert into aggregate list
		aggr_index = node.aggregates.size();
		node.aggregate_map.insert(make_pair(aggregate.get(), aggr_index));
		node.aggregates.push_back(move(aggregate));
	} else {
		// duplicate aggregate: simplify refer to this aggregate
		aggr_index = entry->second;
	}

	// now create a column reference referring to the aggregate
	auto colref = make_unique<BoundColumnRefExpression>(
	    aggr.alias.empty() ? node.aggregates[aggr_index]->ToString() : aggr.alias,
	    node.aggregates[aggr_index]->return_type, ColumnBinding(node.aggregate_index, aggr_index), depth);
	// move the aggregate expression into the set of bound aggregates
	return BindResult(move(colref));
}
} // namespace duckdb
