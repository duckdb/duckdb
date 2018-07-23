
#include "execution/expression_executor.hpp"

#include "common/exception.hpp"

#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/crossproduct_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/join_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "parser/expression/tableref_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(AbstractExpression* expr, Vector& result) {
	expr->Accept(this);

	if (result.type != vector.type) {
		throw NotImplementedException("FIXME: cast");
	}
	vector.Move(result);
}

void ExpressionExecutor::Visit(AggregateExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(BaseTableRefExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(ColumnRefExpression &expr) {
	throw NotImplementedException("XX");
}

void ExpressionExecutor::Visit(ComparisonExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(ConjunctionExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(ConstantExpression &expr) {
	Vector v(expr.value);
	v.Move(vector);
}

void ExpressionExecutor::Visit(CrossProductExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(FunctionExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(JoinExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(OperatorExpression &expr) {
	if (expr.children.size() == 1) {
		Vector l;
		expr.children[0]->Accept(this);
		vector.Move(l);


	} else if (expr.children.size() == 2) {
		Vector l, r, result;
		expr.children[0]->Accept(this);
		vector.Move(l);
		expr.children[1]->Accept(this);
		vector.Move(r);

		vector.Resize(std::max(l.count, r.count));

		switch(expr.type) {
			case ExpressionType::OPERATOR_PLUS:
				Vector::Add(l, r, vector);
				break;
			case ExpressionType::OPERATOR_MINUS:
				Vector::Subtract(l, r, vector);
				break;
			case ExpressionType::OPERATOR_MULTIPLY:
				Vector::Multiply(l, r, vector);
				break;
			case ExpressionType::OPERATOR_DIVIDE:
				Vector::Divide(l, r, vector);
				break;
			default:
				throw NotImplementedException("operator");
		}
	} else {
		throw NotImplementedException("operator");
	}
}

void ExpressionExecutor::Visit(SubqueryExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(TableRefExpression &expr) {
	throw NotImplementedException("");
}
