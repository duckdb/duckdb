#include "planner/expression_binder.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/query_node/select_node.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context, SelectNode& node) :
	binder(binder), context(context), node(node) {
	
}

BindResult ExpressionBinder::BindColumnRefExpression(unique_ptr<Expression> expr) {
	assert(expr->GetExpressionClass() == ExpressionClass::COLUMN_REF);
	auto &colref = (ColumnRefExpression&) *expr;

	assert(!colref.column_name.empty());
	// individual column reference
	// resolve to either a base table or a subquery expression
	if (colref.table_name.empty()) {
		// no table name: find a binding that contains this
		colref.table_name = binder.bind_context.GetMatchingBinding(colref.column_name);
		if (colref.table_name.empty()) {
			return BindResult(move(expr), StringUtil::Format("Referenced column \"%s\" not found in FROM clause!", colref.column_name.c_str()));
		}
	}
	return binder.bind_context.BindColumn(move(expr));
}

BindResult ExpressionBinder::BindFunctionExpression(unique_ptr<Expression> expr) {
	assert(expr->GetExpressionClass() == ExpressionClass::FUNCTION);
	// bind the children of the function expression
	auto result = BindChildren(move(expr));
	if (result.HasError()) {
		return result;
	}
	auto &function = (FunctionExpression&) *result.expression;

	// lookup the function in the catalog
	auto func = context.db.catalog.GetScalarFunction(context.ActiveTransaction(), function.schema, function.function_name);
	// if found, construct the BoundFunctionExpression
	auto func_expr = unique_ptr_cast<Expression, FunctionExpression>(move(result.expression));
	return BindResult(make_unique<BoundFunctionExpression>(move(func_expr), func));
}

BindResult ExpressionBinder::BindSubqueryExpression(unique_ptr<Expression> expr) {
	// bind columns in the subquery
	Binder subquery_binder(context, &binder);
	// the subquery may refer to CTEs from the parent query
	subquery_binder.CTE_bindings = binder.CTE_bindings;

	throw BinderException("Bind subquery");
	// binder.Bind(*expr.subquery);
	// auto &select_list = expr.subquery->GetSelectList();
	// if (select_list.size() < 1) {
	// 	throw BinderException("Subquery has no projections");
	// }
	// if (expr.subquery_type != SubqueryType::EXISTS && select_list.size() != 1) {
	// 	throw BinderException("Subquery returns %zu columns - expected 1", select_list.size());
	// }
	// assert(select_list[0]->return_type != TypeId::INVALID); // "Subquery has no type"
	// auto result = make_unique<BoundSubqueryExpression>();
	// result->return_type = expr.subquery_type == SubqueryType::SCALAR ? select_list[0]->return_type : expr.return_type;
	// result->context = move(binder.bind_context);
	// result->is_correlated = result->context->GetMaxDepth() > 0;
	// result->subquery = move(*expr_ptr);
	// result->alias = expr.alias;
	// return result;
}

BindResult ExpressionBinder::BindChildren(unique_ptr<Expression> expr) {
	string error;
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		if (!error.empty()) {
			return child;
		}
		auto result = BindExpression(move(child));
		if (result.HasError()) {
			error = result.error;
		}
		return move(result.expression);
	});
	return BindResult(move(expr), error);
}

void ExpressionBinder::BindAndResolveType(unique_ptr<Expression>* expr) {
	binder.active_binder = this;
	auto result = BindExpression(move(*expr));
	if (result.HasError()) {
		throw BinderException(result.error);
	}
	result.expression->ResolveType();
	assert(result.expression->return_type != TypeId::INVALID);
	*expr = move(result.expression);
	binder.active_binder = nullptr;
}