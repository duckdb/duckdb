#include "planner/expression_binder/having_binder.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"
#include "main/client_context.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"

using namespace duckdb;
using namespace std;

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map) : 
	SelectBinder(binder, context, node, group_map) {

}

BindResult HavingBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	// check if the expression binds to one of the groups
	auto group_binding = TryBindGroup(expr.get(), depth);
	if (group_binding) {
		return BindResult(move(group_binding));
	}
	switch(expr->GetExpressionClass()) {
		case ExpressionClass::WINDOW:
			return BindResult(move(expr), "HAVING clause cannot contain window functions!");
		case ExpressionClass::FUNCTION:
			return BindFunctionExpression(move(expr), depth);
		case ExpressionClass::SUBQUERY:
			return BindSubqueryExpression(move(expr), depth);
		case ExpressionClass::AGGREGATE:
			return BindAggregate(move(expr), depth);
		case ExpressionClass::COLUMN_REF: {
			string error = StringUtil::Format("column %s must appear in the GROUP BY clause or be used in an aggregate function", expr->ToString().c_str());
			return BindResult(move(expr), error);
		}
		default:
			return BindChildren(move(expr), depth);
	}
}
