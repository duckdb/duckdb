#include "parser/expression/operator_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

static SQLType ResolveNotType(OperatorExpression &op, vector<BoundExpression *> &children) {
	// NOT expression, cast child to BOOLEAN
	assert(children.size() == 1);
	children[0]->expr = AddCastToType(move(children[0]->expr), children[0]->sql_type, SQLType(SQLTypeId::BOOLEAN));
	return SQLType(SQLTypeId::BOOLEAN);
}

static SQLType ResolveInType(OperatorExpression &op, vector<BoundExpression *> &children) {
	// get the maximum type from the children
	SQLType max_type = children[0]->sql_type;
	for (index_t i = 1; i < children.size(); i++) {
		max_type = MaxSQLType(max_type, children[i]->sql_type);
	}
	// cast all children to the same type
	for (index_t i = 0; i < children.size(); i++) {
		children[i]->expr = AddCastToType(move(children[i]->expr), children[i]->sql_type, max_type);
	}
	// (NOT) IN always returns a boolean
	return SQLType(SQLTypeId::BOOLEAN);
}

static SQLType ResolveAddType(OperatorExpression &op, vector<BoundExpression *> &children) {
	switch (children[0]->sql_type.id) {
	case SQLTypeId::DATE:
		switch (children[1]->sql_type.id) {
		case SQLTypeId::TINYINT:
		case SQLTypeId::SMALLINT:
		case SQLTypeId::INTEGER:
		case SQLTypeId::BIGINT:
			// integers can be added to dates, the result is a date again
			// need to cast child to INTEGER
			children[1]->expr =
			    AddCastToType(move(children[1]->expr), children[1]->sql_type, SQLType(SQLTypeId::INTEGER));
			return SQLType(SQLTypeId::DATE);
		default:
			break;
		}
		break;
	default:
		break;
	}
	throw BinderException("Unimplemented types for addition: %s + %s", SQLTypeToString(children[0]->sql_type).c_str(),
	                      SQLTypeToString(children[1]->sql_type).c_str());
}

static SQLType ResolveSubtractType(OperatorExpression &op, vector<BoundExpression *> &children) {
	switch (children[0]->sql_type.id) {
	case SQLTypeId::DATE:
		switch (children[1]->sql_type.id) {
		case SQLTypeId::DATE:
			// dates can be subtracted from dates, the result is an integer (amount of days)
			return SQLType(SQLTypeId::INTEGER);
		case SQLTypeId::TINYINT:
		case SQLTypeId::SMALLINT:
		case SQLTypeId::INTEGER:
		case SQLTypeId::BIGINT:
			// integers can be subtracted from dates, the result is a date again
			// need to cast child to INTEGER
			children[1]->expr =
			    AddCastToType(move(children[1]->expr), children[1]->sql_type, SQLType(SQLTypeId::INTEGER));
			return SQLType(SQLTypeId::DATE);
		default:
			break;
		}
		break;
	default:
		break;
	}
	throw BinderException("Unimplemented types for subtract: %s - %s", SQLTypeToString(children[0]->sql_type).c_str(),
	                      SQLTypeToString(children[1]->sql_type).c_str());
}

static SQLType ResolveMultiplyType(OperatorExpression &op, vector<BoundExpression *> &children) {
	throw BinderException("Unimplemented types for divide: %s * %s", SQLTypeToString(children[0]->sql_type).c_str(),
	                      SQLTypeToString(children[1]->sql_type).c_str());
}

static SQLType ResolveDivideType(OperatorExpression &op, vector<BoundExpression *> &children) {
	throw BinderException("Unimplemented types for divide: %s / %s", SQLTypeToString(children[0]->sql_type).c_str(),
	                      SQLTypeToString(children[1]->sql_type).c_str());
}

static SQLType ResolveModuloType(OperatorExpression &op, vector<BoundExpression *> &children) {
	throw BinderException("Unimplemented types for modulo: %s mod %s", SQLTypeToString(children[0]->sql_type).c_str(),
	                      SQLTypeToString(children[1]->sql_type).c_str());
}

static SQLType ResolveArithmeticType(OperatorExpression &op, vector<BoundExpression *> &children) {
	assert(children.size() == 2);
	auto left_type = children[0]->sql_type;
	auto right_type = children[1]->sql_type;
	if (IsNumericType(left_type.id) && IsNumericType(right_type.id)) {
		// both are numeric, return the max type and cast the children
		auto result_type = MaxSQLType(left_type, right_type);
		children[0]->expr = AddCastToType(move(children[0]->expr), children[0]->sql_type, result_type);
		children[1]->expr = AddCastToType(move(children[1]->expr), children[1]->sql_type, result_type);
		return result_type;
	}
	if (left_type.id == SQLTypeId::SQLNULL) {
		children[0]->expr = AddCastToType(move(children[0]->expr), children[0]->sql_type, right_type);
		return right_type.id;
	} else if (right_type.id == SQLTypeId::SQLNULL) {
		children[1]->expr = AddCastToType(move(children[1]->expr), children[1]->sql_type, left_type);
		return left_type.id;
	}

	if (children[0]->expr->expression_class == ExpressionClass::BOUND_PARAMETER &&
	    children[1]->expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		throw BinderException("Could not resolve type for operator");
	}
	// always cast parameters to the type of the other expression
	if (children[0]->expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		children[0]->expr = AddCastToType(move(children[0]->expr), children[0]->sql_type, children[1]->sql_type);
		return children[1]->sql_type;
	} else if (children[1]->expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		children[1]->expr = AddCastToType(move(children[1]->expr), children[1]->sql_type, children[0]->sql_type);
		return children[0]->sql_type;
	}
	// non-numeric types in arithmetic operator, use per-operator type handling
	switch (op.type) {
	case ExpressionType::OPERATOR_ADD:
		return ResolveAddType(op, children);
	case ExpressionType::OPERATOR_SUBTRACT:
		return ResolveSubtractType(op, children);
	case ExpressionType::OPERATOR_MULTIPLY:
		return ResolveMultiplyType(op, children);
	case ExpressionType::OPERATOR_DIVIDE:
		return ResolveDivideType(op, children);
	case ExpressionType::OPERATOR_LSHIFT:
	case ExpressionType::OPERATOR_RSHIFT:
	case ExpressionType::OPERATOR_BITWISE_AND:
	case ExpressionType::OPERATOR_BITWISE_OR:
	case ExpressionType::OPERATOR_BITWISE_XOR:
		throw BinderException("Unimplemented types for bitwise operator: %s bitop %s",
		                      SQLTypeToString(children[0]->sql_type).c_str(),
		                      SQLTypeToString(children[1]->sql_type).c_str());
	default:
		assert(op.type == ExpressionType::OPERATOR_MOD);
		return ResolveModuloType(op, children);
	}
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
	case ExpressionType::OPERATOR_ADD:
	case ExpressionType::OPERATOR_SUBTRACT:
	case ExpressionType::OPERATOR_MULTIPLY:
	case ExpressionType::OPERATOR_DIVIDE:
	case ExpressionType::OPERATOR_MOD:
	case ExpressionType::OPERATOR_LSHIFT:
	case ExpressionType::OPERATOR_RSHIFT:
	case ExpressionType::OPERATOR_BITWISE_AND:
	case ExpressionType::OPERATOR_BITWISE_OR:
	case ExpressionType::OPERATOR_BITWISE_XOR:
		return ResolveArithmeticType(op, children);
	default:
		assert(op.type == ExpressionType::OPERATOR_NOT);
		return ResolveNotType(op, children);
	}
}

BindResult ExpressionBinder::BindExpression(OperatorExpression &op, index_t depth) {
	// bind the children of the operator expression
	string error;
	for (index_t i = 0; i < op.children.size(); i++) {
		BindChild(op.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully, extract them
	vector<BoundExpression *> children;
	for (index_t i = 0; i < op.children.size(); i++) {
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
