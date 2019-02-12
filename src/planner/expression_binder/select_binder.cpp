#include "planner/expression_binder/select_binder.hpp"
#include "main/client_context.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"

using namespace duckdb;
using namespace std;

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map, bool has_aggregation) : 
	SelectNodeBinder(binder, context, node), inside_aggregation(false), inside_window(false), has_aggregation(has_aggregation), group_map(group_map) {
	
}

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map) : 
	SelectBinder(binder, context, node, group_map, node.HasAggregation()) {

}

BindResult SelectBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	if (has_aggregation && !inside_aggregation) {
		// there is an aggregation and we are not inside the aggregation
		// check if the expression points to one of the groups
		auto entry = group_map.find(expr.get());
		if (entry != group_map.end()) {
			// it does! create a binding to that entry in the group list
			return BindResult(make_unique<BoundColumnRefExpression>(*expr, node.groupby.groups[entry->second]->return_type, ColumnBinding(node.binding.group_index, entry->second), depth));
		}
	}
	switch(expr->GetExpressionClass()) {
		case ExpressionClass::FUNCTION:
			return BindFunctionExpression(move(expr), depth);
		case ExpressionClass::AGGREGATE: {
			if (inside_aggregation) {
				return BindResult(move(expr), "aggregate function calls cannot be nested");
			}
			if (depth > 0) {
				// we are trying to bind an aggregation that might have correlated columns with this query
				// check if we can pull all children of the aggregate in here
				// if we can, we need to pull the aggregation inside this node
				throw NotImplementedException("bla");
				// check if there is already a BoundColumnRefExpression
				// bool has_bound_columnref = HasBoundColumnRefExpression(*expr);
				// if (has_bound_columnref) {

				// } else {
				// 	has_aggregation = true;

				// }
			} else {
				// create a new binder to bind the children of the aggregation
				SelectBinder aggregate_binder(binder, context, node, group_map, has_aggregation);
				aggregate_binder.inside_aggregation = true;

				binder.GetActiveBinders().back() = &aggregate_binder;
				auto bind_result = aggregate_binder.BindChildren(move(expr), depth);
				binder.GetActiveBinders().back() = this;
				if (bind_result.HasError()) {
					// failed to bind children of aggregation
					return bind_result;
				}
				bind_result.expression->ResolveType();
				// create a BoundColumnRef that references this entry
				auto colref = make_unique<BoundColumnRefExpression>(*bind_result.expression, bind_result.expression->return_type, ColumnBinding(node.binding.aggregate_index, node.binding.aggregates.size()), depth);
				// move the aggregate expression into the set of bound aggregates
				node.binding.aggregates.push_back(move(bind_result.expression));
				return BindResult(move(colref));
			}
		}
		case ExpressionClass::WINDOW: {
			if (inside_aggregation) {
				return BindResult(move(expr), "aggregate function calls cannot contain window function calls");
			}
			if (inside_window) {
				return BindResult(move(expr), "window function calls cannot be nested");
			}
			if (depth > 0) {
				return BindResult(move(expr), "correlated columns in window functions not supported");
			}
			// create a new binder to bind the children of the window function
			SelectBinder window_binder(binder, context, node, group_map, has_aggregation);
			window_binder.inside_window = true;

			binder.GetActiveBinders().back() = &window_binder;
			auto bind_result = window_binder.BindChildren(move(expr), depth);
			binder.GetActiveBinders().back() = this;
			if (bind_result.HasError()) {
				// failed to bind children of window function
				return bind_result;
			}
			bind_result.expression->ResolveType();
			// create a BoundColumnRef that references this entry
			auto colref = make_unique<BoundColumnRefExpression>(*bind_result.expression, bind_result.expression->return_type, ColumnBinding(node.binding.window_index, node.binding.windows.size()), depth);
			// move the WINDOW expression into the set of bound windows
			node.binding.windows.push_back(move(bind_result.expression));
			return BindResult(move(colref));
		}
		case ExpressionClass::COLUMN_REF:
			if (has_aggregation && !inside_aggregation) {
				// there is an aggregation and we are NOT inside the aggregation
				// CONTROVERSIAL: in PostgreSQL this would be an error, but SQLite accepts it
				// we try to bind the expression by first wrapping the column inside a FIRST aggregate
				auto first_aggregate = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, expr->Copy());
				// now bind the FIRST aggregate expression
				auto result = BindExpression(move(first_aggregate), depth);
				if (!result.HasError()) {
					// succeeded, return the result
					return result;
				}
				// otherwise we move the original expression back
				return BindResult(move(expr), result.error);
			} else {
				// either (1) there is no aggregation or (2) we are inside an aggregation already
				// use the normal TableBinder to bind it
				return BindColumnRefExpression(move(expr), depth);
			}
		default:
			return BindChildren(move(expr), depth);
	}
}
