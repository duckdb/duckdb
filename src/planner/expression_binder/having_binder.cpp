#include "planner/expression_binder/having_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"
#include "main/client_context.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"

using namespace duckdb;
using namespace std;

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map) : 
	SelectNodeBinder(binder, context, node), group_map(group_map) {

}

BindResult HavingBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	// check if the expression points to one of the groups
	auto entry = group_map.find(expr.get());
	if (entry != group_map.end()) {
		// it does! create a binding to that entry in the group list
		return BindResult(make_unique<BoundColumnRefExpression>(*expr, node.groupby.groups[entry->second]->return_type, ColumnBinding(node.binding.group_index, entry->second), depth));
	}
	switch(expr->GetExpressionClass()) {
		case ExpressionClass::FUNCTION:
			return BindFunctionExpression(move(expr), depth);
		case ExpressionClass::WINDOW:
			return BindResult(move(expr), "HAVING clause cannot contain window functions!");
		case ExpressionClass::AGGREGATE: {
			// FIXME: duplicate of equivalent code in SelectBinder::
			// aggregate expression
			// create a new binder to bind the children of the aggregation
			SelectBinder aggregate_binder(binder, context, node, group_map, true);
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
		case ExpressionClass::COLUMN_REF:{
			// FIXME: duplocate of equivalent code in SelectBinder::
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
		}
		default:
			return BindChildren(move(expr), depth);
	}
}
