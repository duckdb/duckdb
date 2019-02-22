//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class SelectNode;

struct BindResult {
	BindResult(unique_ptr<Expression> expr, string error) : expression(move(expr)), error(error) {
	}
	BindResult(unique_ptr<Expression> expr) : expression(move(expr)) {
	}

	bool HasError() {
		return !error.empty();
	}

	unique_ptr<Expression> expression;
	string error;
};

class ExpressionBinder {
public:
	ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder = false);
	virtual ~ExpressionBinder();

	virtual BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth) = 0;

	BindResult BindColumnRefExpression(unique_ptr<Expression> expr, uint32_t depth);
	BindResult BindFunctionExpression(unique_ptr<Expression> expr, uint32_t depth);
	BindResult BindSubqueryExpression(unique_ptr<Expression> expr, uint32_t depth);
	BindResult BindChildren(unique_ptr<Expression> expr, uint32_t depth);

	void BindAndResolveType(unique_ptr<Expression> *expr);
	BindResult TryBindAndResolveType(unique_ptr<Expression> expr);

protected:
	void ExtractCorrelatedExpressions(Binder &binder, Expression &expr);

	BindResult BindCorrelatedColumns(BindResult result, bool bind_only_children = false);

	Binder &binder;
	ClientContext &context;
	ExpressionBinder *stored_binder;

private:
};

class SelectNodeBinder : public ExpressionBinder {
public:
	SelectNodeBinder(Binder &binder, ClientContext &context, SelectNode &node, bool replace_binder = false)
	    : ExpressionBinder(binder, context, replace_binder), node(node) {
	}

protected:
	SelectNode &node;
};

} // namespace duckdb
