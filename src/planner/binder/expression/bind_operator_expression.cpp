#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

static SQLType ResolveNotType(OperatorExpression &op, vector<BoundExpression *> &children) {
	// NOT expression, cast child to BOOLEAN
	assert(children.size() == 1);
	children[0]->expr =
	    BoundCastExpression::AddCastToType(move(children[0]->expr), children[0]->sql_type, SQLType(SQLTypeId::BOOLEAN));
	return SQLType(SQLTypeId::BOOLEAN);
}

static SQLType ResolveInType(OperatorExpression &op, vector<BoundExpression *> &children) {
	// get the maximum type from the children
	SQLType max_type = children[0]->sql_type;
	for (idx_t i = 1; i < children.size(); i++) {
		max_type = MaxSQLType(max_type, children[i]->sql_type);
	}
	// cast all children to the same type
	for (idx_t i = 0; i < children.size(); i++) {
		children[i]->expr =
		    BoundCastExpression::AddCastToType(move(children[i]->expr), children[i]->sql_type, max_type);
	}
	// (NOT) IN always returns a boolean
	return SQLType(SQLTypeId::BOOLEAN);
}

static SQLType ResolveOperatorType(OperatorExpression &op, vector<BoundExpression *> &children) {
	switch (op.type) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		// IS (NOT) NULL always returns a boolean, and does not cast its children
		return SQLType(SQLTypeId::BOOLEAN);
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN:
		return ResolveInType(op, children);
	default:
		assert(op.type == ExpressionType::OPERATOR_NOT);
		return ResolveNotType(op, children);
	}
}

BindResult ExpressionBinder::BindExpression(OperatorExpression &op, idx_t depth) {
	// bind the children of the operator expression
	string error;
	for (idx_t i = 0; i < op.children.size(); i++) {
		BindChild(op.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully, extract them
	vector<BoundExpression *> children;
	for (idx_t i = 0; i < op.children.size(); i++) {
		assert(op.children[i]->expression_class == ExpressionClass::BOUND_EXPRESSION);
		children.push_back((BoundExpression *)op.children[i].get());
	}
	// now resolve the types
	SQLType result_type = ResolveOperatorType(op, children);

	auto result = make_unique<BoundOperatorExpression>(op.type, GetInternalType(result_type));
	for (auto &child : children) {
		result->children.push_back(move(child->expr));
	}
	return BindResult(move(result), result_type);
}
