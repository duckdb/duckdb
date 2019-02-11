#include "planner/expression_binder/select_binder.hpp"
#include "main/client_context.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"

using namespace duckdb;
using namespace std;

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map, bool has_aggregation) : 
	ExpressionBinder(binder, context, node), inside_aggregation(false), inside_window(false), has_aggregation(has_aggregation), group_map(group_map) {
	
}

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map) : 
	SelectBinder(binder, context, node, group_map, node.HasAggregation()) {

}


BindResult SelectBinder::BindExpression(unique_ptr<Expression> expr) {
	if (has_aggregation && !inside_aggregation) {
		// there is an aggregation and we are not inside the aggregation
		// check if the expression points to one of the groups
		auto entry = group_map.find(expr.get());
		if (entry != group_map.end()) {
			// it does! create a binding to that entry in the group list
			return BindResult(make_unique<BoundColumnRefExpression>(*expr, node.groupby.groups[entry->second]->return_type, ColumnBinding(node.binding.group_index, entry->second)));
		}
	}
	switch(expr->GetExpressionClass()) {
		case ExpressionClass::AGGREGATE: {
			if (inside_aggregation) {
				return BindResult(move(expr), "aggregate function calls cannot be nested");
			}
			// create a new binder to bind the children of the aggregation
			SelectBinder aggregate_binder(binder, context, node, group_map, has_aggregation);
			aggregate_binder.inside_aggregation = true;
			auto bind_result = aggregate_binder.BindChildren(move(expr));
			if (bind_result.HasError()) {
				// failed to bind children of aggregation
				return bind_result;
			}
			bind_result.expression->ResolveType();
			// create a BoundColumnRef that references this entry
			auto colref = make_unique<BoundColumnRefExpression>(*bind_result.expression, bind_result.expression->return_type, ColumnBinding(node.binding.aggregate_index, node.binding.aggregates.size()));
			// move the aggregate expression into the set of bound aggregates
			node.binding.aggregates.push_back(move(bind_result.expression));
			return BindResult(move(colref));
		}
		case ExpressionClass::WINDOW: {
			if (inside_aggregation) {
				return BindResult(move(expr), "aggregate function calls cannot contain window function calls");
			}
			if (inside_window) {
				return BindResult(move(expr), "window function calls cannot be nested");
			}
			// create a new binder to bind the children of the window function
			SelectBinder window_binder(binder, context, node, group_map, has_aggregation);
			window_binder.inside_window = true;
			auto bind_result = window_binder.BindChildren(move(expr));
			if (bind_result.HasError()) {
				// failed to bind children of window function
				return bind_result;
			}
			bind_result.expression->ResolveType();
			// create a BoundColumnRef that references this entry
			auto colref = make_unique<BoundColumnRefExpression>(*bind_result.expression, bind_result.expression->return_type, ColumnBinding(node.binding.window_index, node.binding.windows.size()));
			// move the WINDOW expression into the set of bound windows
			node.binding.windows.push_back(move(bind_result.expression));
			return BindResult(move(colref));
		}
		case ExpressionClass::COLUMN_REF:
			if (has_aggregation && !inside_aggregation) {
				// there is an aggregation and we are NOT inside the aggregation
				// wrap the column inside a FIRST aggregate
				auto first_aggregate = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, move(expr));
				// now bind the FIRST aggregate expression
				return BindExpression(move(first_aggregate));
			} else {
				// either (1) there is no aggregation or (2) we are inside an aggregation already
				// use the normal TableBinder to bind it
				return BindColumnRefExpression(move(expr));
			}
		case ExpressionClass::FUNCTION:
			return BindFunctionExpression(move(expr));
		case ExpressionClass::SUBQUERY:
			return BindSubqueryExpression(move(expr));
		default:
			return BindChildren(move(expr));
	}
}
