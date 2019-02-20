#include "planner/expression_binder.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/bound_subquery_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder) :
	binder(binder), context(context), stored_binder(nullptr) {
	if (replace_binder) {
		stored_binder = binder.GetActiveBinder();
		binder.SetActiveBinder(this);
	} else {
		binder.PushExpressionBinder(this);
	}
}

ExpressionBinder::~ExpressionBinder() {
	if (binder.HasActiveBinder()) {
		if (stored_binder) {
			binder.SetActiveBinder(stored_binder);
		} else {
			binder.PopExpressionBinder();
		}
	}
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
	auto bind_result = BindChildren(move(expr), depth);
	if (bind_result.HasError()) {
		return bind_result;
	}
	auto &subquery = (SubqueryExpression&) *bind_result.expression;
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
	// check the correlated columns of the subquery for correlated columns with depth > 1
	for(size_t i = 0; i < subquery_binder->correlated_columns.size(); i++) {
		CorrelatedColumnInfo corr = subquery_binder->correlated_columns[i];
		if (corr.depth > 1) {
			// depth > 1, the column references the query ABOVE the current one
			// add to the set of correlated columns for THIS query
			corr.depth -= 1;
			binder.AddCorrelatedColumn(corr);
		}
	}
	assert(select_list[0]->return_type != TypeId::INVALID); // "Subquery has no type"
	// create the bound subquery
	auto result = make_unique<BoundSubqueryExpression>();
	result->return_type = subquery.subquery_type == SubqueryType::SCALAR ? select_list[0]->return_type : subquery.return_type;
	result->binder = move(subquery_binder);
	result->subquery = move(bind_result.expression);
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

BindResult ExpressionBinder::BindCorrelatedColumns(BindResult result, bool bind_only_children) {
	if (!result.HasError()) {
		return result;
	}
	// try to bind in one of the outer queries, if the binding error occurred in a subquery
	auto &active_binders = binder.GetActiveBinders();
	auto binders = active_binders;
	active_binders.pop_back();
	size_t depth = 1;
	while(active_binders.size() > 0) {
		if (bind_only_children) {
			result = active_binders.back()->BindChildren(move(result.expression), depth);
		} else {
			result = active_binders.back()->BindExpression(move(result.expression), depth);
		}
		if (!result.HasError()) {
			break;
		}
		depth++;
		active_binders.pop_back();
	}
	active_binders = binders;
	return result;
}

void ExpressionBinder::ExtractCorrelatedExpressions(Binder &binder, Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression&) expr;
		if (bound_colref.depth > 0) {
			binder.AddCorrelatedColumn(CorrelatedColumnInfo(bound_colref));
		}
	}
	expr.EnumerateChildren([&](Expression *child) {
		ExtractCorrelatedExpressions(binder, *child);
	});
}

BindResult ExpressionBinder::TryBindAndResolveType(unique_ptr<Expression> expr) {
	// bind the main expression
	auto result = BindExpression(move(expr), 0);
	// try to bind correlated columns in the expression (if any)
	result = BindCorrelatedColumns(move(result));
	if (result.HasError()) {
		return result;
	}
	ExtractCorrelatedExpressions(binder, *result.expression);
	result.expression->ResolveType();
	assert(result.expression->return_type != TypeId::INVALID);
	return BindResult(move(result.expression));
}

void ExpressionBinder::BindAndResolveType(unique_ptr<Expression>* expr) {
	auto result = TryBindAndResolveType(move(*expr));
	if (result.HasError()) {
		throw BinderException(result.error);
	}
	*expr = move(result.expression);
}
