#include "planner/expression_binder.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/query_node/select_node.hpp"
#include "parser/expression/bound_subquery_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context, SelectNode& node) :
	binder(binder), context(context), node(node) {
	
}

BindResult ExpressionBinder::BindColumnRefExpression(unique_ptr<Expression> expr, uint32_t depth) {
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
	return binder.bind_context.BindColumn(move(expr), depth);
}

BindResult ExpressionBinder::BindSubqueries(unique_ptr<Expression> expr, uint32_t depth) {
	string error;
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		auto result = BindSubqueries(move(child), depth);
		if (result.HasError()) {
			error = result.error;
		}
		return move(result.expression);
	});
	if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		return BindSubqueryExpression(move(expr), depth);
	}
	return BindResult(move(expr), error);
}

BindResult ExpressionBinder::BindFunctionExpression(unique_ptr<Expression> expr, uint32_t depth) {
	assert(expr->GetExpressionClass() == ExpressionClass::FUNCTION);
	// bind the children of the function expression
	auto result = BindChildren(move(expr), depth);
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

BindResult ExpressionBinder::BindSubqueryExpression(unique_ptr<Expression> expr, uint32_t depth) {
	assert(expr->GetExpressionClass() == ExpressionClass::SUBQUERY);
	// first bind the children of the subquery, if any
	auto &subquery = (SubqueryExpression&) *expr;
	// bind columns in the subquery
	auto subquery_binder = make_unique<Binder>(context, &binder);
	// the subquery may refer to CTEs from the parent query
	subquery_binder->CTE_bindings = binder.CTE_bindings;
	subquery_binder->Bind(*subquery.subquery);
	auto &select_list = subquery.subquery->GetSelectList();
	if (select_list.size() < 1) {
		throw BinderException("Subquery has no projections");
	}
	if (subquery.subquery_type != SubqueryType::EXISTS && select_list.size() != 1) {
		throw BinderException("Subquery returns %zu columns - expected 1", select_list.size());
	}
	assert(select_list[0]->return_type != TypeId::INVALID); // "Subquery has no type"
	auto result = make_unique<BoundSubqueryExpression>();
	result->return_type = subquery.subquery_type == SubqueryType::SCALAR ? select_list[0]->return_type : subquery.return_type;
	result->binder = move(subquery_binder);
	result->subquery = move(expr);
	result->alias = subquery.alias;
	return BindResult(move(result));
}

BindResult ExpressionBinder::BindChildren(unique_ptr<Expression> expr, uint32_t depth) {
	string error;
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		auto result = BindExpression(move(child), depth);
		if (result.HasError()) {
			error = result.error;
		}
		return move(result.expression);
	});
	return BindResult(move(expr), error);
}

void ExpressionBinder::BindAndResolveType(unique_ptr<Expression>* expr) {
	auto result = TryBindAndResolveType(move(*expr));
	if (result.HasError()) {
		throw BinderException(result.error);
	}
	*expr = move(result.expression);
}

static void ExtractCorrelatedExpressions(Binder &binder, Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression&) expr;
		if (bound_colref.depth > 0) {
			binder.correlated_columns.push_back(CorrelatedColumnInfo(bound_colref));
		}
	}
	expr.EnumerateChildren([&](Expression *child) {
		ExtractCorrelatedExpressions(binder, *child);
	});
}


BindResult ExpressionBinder::TryBindAndResolveType(unique_ptr<Expression> expr) {
	binder.PushExpressionBinder(this);
	auto result = BindSubqueries(move(expr), 0);
	if (result.HasError()) {
		return result;
	}
	result = BindExpression(move(result.expression), 0);
	binder.PopExpressionBinder();
	if (result.HasError()) {
		// try to bind in one of the outer queries, if the binding error occurred in a subquery
		auto &active_binders = binder.GetActiveBinders();
		for(size_t i = active_binders.size(); i > 0; i--) {
			result = active_binders[i - 1]->BindExpression(move(result.expression), active_binders.size() - i + 1);
		}
		if (result.HasError()) {
			return result;
		}
		// extract correlated expressions and place in this binder
		ExtractCorrelatedExpressions(binder, *result.expression);
	}
	result.expression->ResolveType();
	assert(result.expression->return_type != TypeId::INVALID);
	return BindResult(move(result.expression));
}