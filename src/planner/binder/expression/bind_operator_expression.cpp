#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static LogicalType ResolveNotType(OperatorExpression &op, vector<BoundExpression *> &children) {
	// NOT expression, cast child to BOOLEAN
	D_ASSERT(children.size() == 1);
	children[0]->expr = BoundCastExpression::AddCastToType(move(children[0]->expr), LogicalType::BOOLEAN);
	return LogicalType(LogicalTypeId::BOOLEAN);
}

static LogicalType ResolveInType(OperatorExpression &op, vector<BoundExpression *> &children) {
	if (children.empty()) {
		return LogicalType::BOOLEAN;
	}
	// get the maximum type from the children
	LogicalType max_type = children[0]->expr->return_type;
	for (idx_t i = 1; i < children.size(); i++) {
		max_type = LogicalType::MaxLogicalType(max_type, children[i]->expr->return_type);
	}
	// cast all children to the same type
	for (idx_t i = 0; i < children.size(); i++) {
		children[i]->expr = BoundCastExpression::AddCastToType(move(children[i]->expr), max_type);
	}
	// (NOT) IN always returns a boolean
	return LogicalType::BOOLEAN;
}

static LogicalType ResolveOperatorType(OperatorExpression &op, vector<BoundExpression *> &children) {
	switch (op.type) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		// IS (NOT) NULL always returns a boolean, and does not cast its children
		return LogicalType::BOOLEAN;
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN:
	case ExpressionType::OPERATOR_COALESCE:
		return ResolveInType(op, children);
	default:
		D_ASSERT(op.type == ExpressionType::OPERATOR_NOT);
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
	// all children bound successfully
	string function_name;
	switch (op.type) {
	case ExpressionType::ARRAY_EXTRACT:
		function_name = "array_extract";
		break;
	case ExpressionType::ARRAY_SLICE:
		function_name = "array_slice";
		break;
	case ExpressionType::STRUCT_EXTRACT:
		function_name = "struct_extract";
		break;
	case ExpressionType::ARRAY_CONSTRUCTOR:
		function_name = "list_value";
		break;
	default:
		break;
	}
	if (!function_name.empty()) {
		auto function = make_unique<FunctionExpression>(function_name, op.children);
		return BindExpression(*function, depth, nullptr);
	}

	vector<BoundExpression *> children;
	for (idx_t i = 0; i < op.children.size(); i++) {
		D_ASSERT(op.children[i]->expression_class == ExpressionClass::BOUND_EXPRESSION);
		children.push_back((BoundExpression *)op.children[i].get());
	}
	// now resolve the types
	LogicalType result_type = ResolveOperatorType(op, children);
	if (op.type == ExpressionType::OPERATOR_COALESCE) {
		if (children.empty()) {
			return BindResult("COALESCE needs at least one child");
		}
		unique_ptr<Expression> current_node;
		for (size_t i = children.size(); i > 0; i--) {
			auto child = move(children[i - 1]->expr);
			if (!current_node) {
				// no node yet: simply move the child
				current_node = move(child);
			} else {
				// create a case statement
				auto check =
				    make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				check->children.push_back(child->Copy());
				current_node = make_unique<BoundCaseExpression>(move(check), move(child), move(current_node));
			}
		}
		return BindResult(move(current_node));
	}

	auto result = make_unique<BoundOperatorExpression>(op.type, result_type);
	for (auto &child : children) {
		result->children.push_back(move(child->expr));
	}
	return BindResult(move(result));
}

} // namespace duckdb
