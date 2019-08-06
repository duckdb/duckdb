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

static SQLType ValidateReturnType(vector<SQLType> &arguments, AggregateFunctionCatalogEntry *func) {
	auto result = func->return_type(arguments);
	if (result == SQLTypeId::INVALID) {
		// types do not match up, throw exception
		string type_str;
		for (index_t i = 0; i < arguments.size(); i++) {
			if (i > 0) {
				type_str += ", ";
			}
			type_str += SQLTypeToString(arguments[i]);
		}
		throw BinderException("Unsupported input types for aggregate %s(%s)", func->name.c_str(), type_str.c_str());
	}
	return result;
}

BindResult SelectBinder::BindAggregate(FunctionExpression &aggr, AggregateFunctionCatalogEntry *func, index_t depth) {
	// first bind the child of the aggregate expression (if any)
	AggregateBinder aggregate_binder(binder, context);
	string error;
	for (index_t i = 0; i < aggr.children.size(); i++) {
		aggregate_binder.BindChild(aggr.children[i], 0, error);
	}
	if (!error.empty()) {
		// failed to bind child
		if (aggregate_binder.BoundColumns()) {
			for (index_t i = 0; i < aggr.children.size(); i++) {
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
		} else {
			// we didn't bind columns, try again in children
			return BindResult(error);
		}
	}
	// all children bound successfully
	// extract the children and types
	vector<SQLType> types;
	vector<unique_ptr<Expression>> children;
	for (index_t i = 0; i < aggr.children.size(); i++) {
		auto &child = (BoundExpression &)*aggr.children[i];
		types.push_back(child.sql_type);
		children.push_back(move(child.expr));
	}

	// types match up, get the result type
	SQLType result_type = ValidateReturnType(types, func);
	// add a cast to the child node (if needed)
	if (func->cast_arguments(types)) {
		for (index_t i = 0; i < children.size(); i++) {
			children[i] = AddCastToType(move(children[i]), types[i], result_type);
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
