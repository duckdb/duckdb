#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/base_select_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

static bool IsFunctionallyDependent(const unique_ptr<Expression> &expr, const vector<unique_ptr<Expression>> &deps) {
	//	Volatile expressions can't depend on anything else
	if (expr->IsVolatile()) {
		return false;
	}
	//	Constant expressions are always FD
	if (expr->IsFoldable()) {
		return true;
	}
	// If the expression matches ANY of the dependencies, then it is FD on them
	for (const auto &dep : deps) {
		// We don't need to check volatility of the dependencies because we checked it for the expression.
		if (expr->Equals(*dep)) {
			return true;
		}
	}

	// The expression doesn't match any dependency, so check ALL children.
	bool has_children = false;
	bool are_dependent = true;
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		has_children = true;
		are_dependent &= IsFunctionallyDependent(child, deps);
	});
	return has_children && are_dependent;
}

static Value NegatePercentileValue(const Value &v, const bool desc) {
	if (v.IsNull()) {
		return v;
	}

	const auto frac = v.GetValue<double>();
	if (frac < 0 || frac > 1) {
		throw BinderException("PERCENTILEs can only take parameters in the range [0, 1]");
	}

	if (!desc) {
		return v;
	}

	const auto &type = v.type();
	switch (type.id()) {
	case LogicalTypeId::DECIMAL: {
		// Negate DECIMALs as DECIMAL.
		const auto integral = IntegralValue::Get(v);
		const auto width = DecimalType::GetWidth(type);
		const auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(Cast::Operation<hugeint_t, int16_t>(-integral), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(Cast::Operation<hugeint_t, int32_t>(-integral), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(Cast::Operation<hugeint_t, int64_t>(-integral), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(-integral, width, scale);
		default:
			throw InternalException("Unknown DECIMAL type");
		}
	}
	default:
		// Everything else can just be a DOUBLE
		return Value::DOUBLE(-v.GetValue<double>());
	}
}

static void NegatePercentileFractions(ClientContext &context, unique_ptr<ParsedExpression> &fractions, bool desc) {
	D_ASSERT(fractions.get());
	D_ASSERT(fractions->expression_class == ExpressionClass::BOUND_EXPRESSION);
	auto &bound = BoundExpression::GetExpression(*fractions);

	if (!bound->IsFoldable()) {
		return;
	}

	Value value = ExpressionExecutor::EvaluateScalar(context, *bound);
	if (value.type().id() == LogicalTypeId::LIST) {
		vector<Value> values;
		for (const auto &element_val : ListValue::GetChildren(value)) {
			values.push_back(NegatePercentileValue(element_val, desc));
		}
		if (values.empty()) {
			throw BinderException("Empty list in percentile not allowed");
		}
		bound = make_uniq<BoundConstantExpression>(Value::LIST(values));
	} else {
		bound = make_uniq<BoundConstantExpression>(NegatePercentileValue(value, desc));
	}
}

BindResult BaseSelectBinder::BindAggregate(FunctionExpression &aggr, AggregateFunctionCatalogEntry &func, idx_t depth) {
	// first bind the child of the aggregate expression (if any)
	this->bound_aggregate = true;
	unique_ptr<Expression> bound_filter;
	AggregateBinder aggregate_binder(binder, context);
	ErrorData error;

	// Now we bind the filter (if any)
	if (aggr.filter) {
		aggregate_binder.BindChild(aggr.filter, 0, error);
	}

	// Handle ordered-set aggregates by moving the single ORDER BY expression to the front of the children.
	//	https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE
	// We also have to handle ORDER BY in the argument list, so note how many arguments we should have
	// and only inject the ordering expression if there are too few.
	idx_t ordered_set_agg = 0;
	bool negate_fractions = false;
	if (aggr.order_bys && aggr.order_bys->orders.size() == 1) {
		const auto &func_name = aggr.function_name;
		if (func_name == "mode") {
			ordered_set_agg = 1;
		} else if (func_name == "quantile_cont" || func_name == "quantile_disc") {
			ordered_set_agg = 2;

			auto &config = DBConfig::GetConfig(context);
			const auto &order = aggr.order_bys->orders[0];
			const auto sense =
			    (order.type == OrderType::ORDER_DEFAULT) ? config.options.default_order_type : order.type;
			negate_fractions = (sense == OrderType::DESCENDING);
		}
	}

	for (idx_t i = 0; i < aggr.children.size(); ++i) {
		auto &child = aggr.children[i];
		aggregate_binder.BindChild(child, 0, error);
		// We have to negate the fractions for PERCENTILE_XXXX DESC
		if (!error.HasError() && ordered_set_agg && i == aggr.children.size() - 1) {
			NegatePercentileFractions(context, child, negate_fractions);
		}
	}

	// Bind the ORDER BYs, if any
	if (aggr.order_bys && !aggr.order_bys->orders.empty()) {
		for (auto &order : aggr.order_bys->orders) {
			if (order.expression->type == ExpressionType::VALUE_CONSTANT) {
				auto &const_expr = order.expression->Cast<ConstantExpression>();
				if (!const_expr.value.type().IsIntegral()) {
					auto &config = ClientConfig::GetConfig(context);
					if (!config.order_by_non_integer_literal) {
						throw BinderException(
						    *order.expression,
						    "ORDER BY non-integer literal has no effect.\n* SET order_by_non_integer_literal=true to "
						    "allow this behavior.\n\nPerhaps you misplaced ORDER BY; ORDER BY must appear "
						    "after all regular arguments of the aggregate.");
					}
				}
			}
			aggregate_binder.BindChild(order.expression, 0, error);
		}
	}

	if (error.HasError()) {
		// failed to bind child
		if (aggregate_binder.HasBoundColumns()) {
			for (idx_t i = 0; i < aggr.children.size(); i++) {
				// however, we bound columns!
				// that means this aggregation belongs to this node
				// check if we have to resolve any errors by binding with parent binders
				auto result = aggregate_binder.BindCorrelatedColumns(aggr.children[i], error);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (result.HasError()) {
					result.error.Throw();
				}
				auto &bound_expr = BoundExpression::GetExpression(*aggr.children[i]);
				ExtractCorrelatedExpressions(binder, *bound_expr);
			}
			if (aggr.filter) {
				auto result = aggregate_binder.BindCorrelatedColumns(aggr.filter, error);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (result.HasError()) {
					result.error.Throw();
				}
				auto &bound_expr = BoundExpression::GetExpression(*aggr.filter);
				ExtractCorrelatedExpressions(binder, *bound_expr);
			}
			if (aggr.order_bys && !aggr.order_bys->orders.empty()) {
				for (auto &order : aggr.order_bys->orders) {
					auto result = aggregate_binder.BindCorrelatedColumns(order.expression, error);
					if (result.HasError()) {
						result.error.Throw();
					}
					auto &bound_expr = BoundExpression::GetExpression(*order.expression);
					ExtractCorrelatedExpressions(binder, *bound_expr);
				}
			}
		} else {
			// we didn't bind columns, try again in children
			return BindResult(std::move(error));
		}
	} else if (depth > 0 && !aggregate_binder.HasBoundColumns()) {
		return BindResult("Aggregate with only constant parameters has to be bound in the root subquery");
	}

	if (aggr.filter) {
		auto &child = BoundExpression::GetExpression(*aggr.filter);
		bound_filter = BoundCastExpression::AddCastToType(context, std::move(child), LogicalType::BOOLEAN);
	}

	// all children bound successfully
	// extract the children and types
	vector<LogicalType> types;
	vector<LogicalType> arguments;
	vector<unique_ptr<Expression>> children;

	if (ordered_set_agg) {
		const bool order_sensitive = (aggr.function_name == "mode");
		// Inject missing ordering arguments
		if (aggr.children.size() < ordered_set_agg) {
			for (auto &order : aggr.order_bys->orders) {
				auto &child = BoundExpression::GetExpression(*order.expression);
				types.push_back(child->return_type);
				arguments.push_back(child->return_type);
				if (order_sensitive) {
					children.push_back(child->Copy());
				} else {
					children.push_back(std::move(child));
				}
			}
		}
		if (!order_sensitive) {
			aggr.order_bys->orders.clear();
		}
	}

	for (idx_t i = 0; i < aggr.children.size(); i++) {
		auto &child = BoundExpression::GetExpression(*aggr.children[i]);
		types.push_back(child->return_type);
		arguments.push_back(child->return_type);
		children.push_back(std::move(child));
	}

	// bind the aggregate
	FunctionBinder function_binder(binder);
	auto best_function = function_binder.BindFunction(func.name, func.functions, types, error);
	if (!best_function.IsValid()) {
		error.AddQueryLocation(aggr);
		error.Throw();
	}
	// found a matching function!
	auto bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());

	// Bind any sort columns, unless the aggregate is order-insensitive
	unique_ptr<BoundOrderModifier> order_bys;
	if (!aggr.order_bys->orders.empty()) {
		order_bys = make_uniq<BoundOrderModifier>();
		auto &config = DBConfig::GetConfig(context);
		for (auto &order : aggr.order_bys->orders) {
			auto &order_expr = BoundExpression::GetExpression(*order.expression);
			PushCollation(context, order_expr, order_expr->return_type);
			const auto sense = config.ResolveOrder(order.type);
			const auto null_order = config.ResolveNullOrder(sense, order.null_order);
			order_bys->orders.emplace_back(sense, null_order, std::move(order_expr));
		}
	}

	// If the aggregate is DISTINCT then the ORDER BYs need to be functional dependencies of the arguments.
	if (aggr.distinct && order_bys) {
		bool in_args = true;
		for (const auto &order_by : order_bys->orders) {
			in_args &= IsFunctionallyDependent(order_by.expression, children);
		}

		if (!in_args) {
			throw BinderException("In a DISTINCT aggregate, ORDER BY expressions must appear in the argument list");
		}
	}

	auto aggregate =
	    function_binder.BindAggregateFunction(bound_function, std::move(children), std::move(bound_filter),
	                                          aggr.distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT);
	if (aggr.export_state) {
		aggregate = ExportAggregateFunction::Bind(std::move(aggregate));
	}
	aggregate->order_bys = std::move(order_bys);

	// check for all the aggregates if this aggregate already exists
	idx_t aggr_index;
	auto entry = node.aggregate_map.find(*aggregate);
	if (entry == node.aggregate_map.end()) {
		// new aggregate: insert into aggregate list
		aggr_index = node.aggregates.size();
		node.aggregate_map[*aggregate] = aggr_index;
		node.aggregates.push_back(std::move(aggregate));
	} else {
		// duplicate aggregate: simplify refer to this aggregate
		aggr_index = entry->second;
	}

	// now create a column reference referring to the aggregate
	auto colref = make_uniq<BoundColumnRefExpression>(
	    aggr.alias.empty() ? node.aggregates[aggr_index]->ToString() : aggr.alias,
	    node.aggregates[aggr_index]->return_type, ColumnBinding(node.aggregate_index, aggr_index), depth);
	// move the aggregate expression into the set of bound aggregates
	return BindResult(std::move(colref));
}
} // namespace duckdb
