#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "main/client_context.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"
#include "planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;

BindResult SelectBinder::BindAggregate(AggregateExpression &aggr, index_t depth) {
	auto aggr_name = aggr.GetName();
	// first bind the child of the aggregate expression (if any)
	unique_ptr<Expression> child;
	SQLType child_type;
	if (aggr.child) {
		AggregateBinder aggregate_binder(binder, context);
		string error = aggregate_binder.Bind(&aggr.child, 0);
		if (!error.empty()) {
			// failed to bind child
			if (aggregate_binder.BoundColumns()) {
				// however, we bound columns!
				// that means this aggregation belongs to this node
				// check if we have to resolve any errors by binding with parent binders
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.child);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.child;
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			} else {
				// we didn't bind columns, try again in children
				return BindResult(error);
			}
		}
		auto &bound_expr = (BoundExpression &)*aggr.child;
		child_type = bound_expr.sql_type;
		child = move(bound_expr.expr);
	}
	// all children bound successfully
	// lookup the function in the catalog
	auto func = context.catalog.GetAggregateFunction(context.ActiveTransaction(), aggr.schema, aggr.aggregate_name);
	// types match up, get the result type
	vector<SQLType> arguments;
	arguments.push_back(child_type);
	SQLType result_type = func->return_type(arguments);
	// add a cast to the child node (if needed)
	if (func->cast_arguments(arguments)) {
		assert(child);
		child = AddCastToType(move(child), child_type, result_type);
	}
	// create the aggregate
	auto aggregate = make_unique<BoundAggregateExpression>(GetInternalType(result_type), aggr.type, move(child), func);
	// now create a column reference referring to this aggregate

	auto colref = make_unique<BoundColumnRefExpression>(
	    aggr_name, aggregate->return_type, ColumnBinding(node.aggregate_index, node.aggregates.size()), depth);
	// move the aggregate expression into the set of bound aggregates
	node.aggregates.push_back(move(aggregate));
	return BindResult(move(colref), result_type);
}
