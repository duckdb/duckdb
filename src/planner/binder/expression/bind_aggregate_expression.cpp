#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "main/client_context.hpp"
#include "parser/expression/function_expression.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"
#include "planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;

BindResult SelectBinder::BindAggregate(FunctionExpression &aggr, AggregateFunctionCatalogEntry *func, index_t depth) {
	// first bind the child of the aggregate expression (if any)
	if (aggr.children.size() > 1) {
		throw ParserException("Aggregates with multiple children not supported");
	}
	vector<unique_ptr<Expression>> children;
	vector<SQLType> child_types;
	for (index_t i = 0; i < aggr.children.size(); i++) {
		AggregateBinder aggregate_binder(binder, context);
		string error = aggregate_binder.Bind(&aggr.children[i], 0);
		if (!error.empty()) {
			// failed to bind child
			if (aggregate_binder.BoundColumns()) {
				// however, we bound columns!
				// that means this aggregation belongs to this node
				// check if we have to resolve any errors by binding with parent binders
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.children[i]);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.children[0];
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			} else {
				// we didn't bind columns, try again in children
				return BindResult(error);
			}
		}
		auto &bound_expr = (BoundExpression &)*aggr.children[0];
		child_types.push_back(bound_expr.sql_type);
		children.push_back(move(bound_expr.expr));
	}
	// all children bound successfully

	// types match up, get the result type
	SQLType result_type = func->return_type(child_types);
	// add a cast to the child node (if needed)
	if (func->cast_arguments(child_types)) {
		for (index_t i = 0; i < children.size(); i++) {
			children[i] = AddCastToType(move(children[i]), child_types[i], result_type);
		}
	}
	// create the aggregate
	auto aggregate = make_unique<BoundAggregateExpression>(GetInternalType(result_type), func, aggr.distinct);
	aggregate->children = move(children);
	// now create a column reference referring to this aggregate

	auto colref = make_unique<BoundColumnRefExpression>(
	    func->name, aggregate->return_type, ColumnBinding(node.aggregate_index, node.aggregates.size()), depth);
	// move the aggregate expression into the set of bound aggregates
	node.aggregates.push_back(move(aggregate));
	return BindResult(move(colref), result_type);
}
