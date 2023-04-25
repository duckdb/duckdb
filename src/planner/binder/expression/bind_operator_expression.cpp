#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static LogicalType ResolveNotType(OperatorExpression &op, vector<unique_ptr<Expression>> &children) {
	// NOT expression, cast child to BOOLEAN
	D_ASSERT(children.size() == 1);
	children[0] = BoundCastExpression::AddDefaultCastToType(std::move(children[0]), LogicalType::BOOLEAN);
	return LogicalType(LogicalTypeId::BOOLEAN);
}

static LogicalType ResolveInType(OperatorExpression &op, vector<unique_ptr<Expression>> &children) {
	if (children.empty()) {
		throw InternalException("IN requires at least a single child node");
	}
	// get the maximum type from the children
	LogicalType max_type = children[0]->return_type;
	bool any_varchar = children[0]->return_type == LogicalType::VARCHAR;
	bool any_enum = children[0]->return_type.id() == LogicalTypeId::ENUM;
	for (idx_t i = 1; i < children.size(); i++) {
		max_type = LogicalType::MaxLogicalType(max_type, children[i]->return_type);
		if (children[i]->return_type == LogicalType::VARCHAR) {
			any_varchar = true;
		}
		if (children[i]->return_type.id() == LogicalTypeId::ENUM) {
			any_enum = true;
		}
	}
	if (any_varchar && any_enum) {
		// For the coalesce function, we must be sure we always upcast the parameters to VARCHAR, if there are at least
		// one enum and one varchar
		max_type = LogicalType::VARCHAR;
	}

	// cast all children to the same type
	for (idx_t i = 0; i < children.size(); i++) {
		children[i] = BoundCastExpression::AddDefaultCastToType(std::move(children[i]), max_type);
	}
	// (NOT) IN always returns a boolean
	return LogicalType::BOOLEAN;
}

static LogicalType ResolveOperatorType(OperatorExpression &op, vector<unique_ptr<Expression>> &children) {
	switch (op.type) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		// IS (NOT) NULL always returns a boolean, and does not cast its children
		if (!children[0]->return_type.IsValid()) {
			throw ParameterNotResolvedException();
		}
		return LogicalType::BOOLEAN;
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN:
		return ResolveInType(op, children);
	case ExpressionType::OPERATOR_COALESCE: {
		ResolveInType(op, children);
		return children[0]->return_type;
	}
	case ExpressionType::OPERATOR_NOT:
		return ResolveNotType(op, children);
	default:
		throw InternalException("Unrecognized expression type for ResolveOperatorType");
	}
}

BindResult ExpressionBinder::BindGroupingFunction(OperatorExpression &op, idx_t depth) {
	return BindResult("GROUPING function is not supported here");
}

BindResult ExpressionBinder::BindExpression(OperatorExpression &op, idx_t depth) {
	if (op.type == ExpressionType::GROUPING_FUNCTION) {
		return BindGroupingFunction(op, depth);
	}
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
	case ExpressionType::ARRAY_EXTRACT: {
		D_ASSERT(op.children[0]->expression_class == ExpressionClass::BOUND_EXPRESSION);
		auto &b_exp = BoundExpression::GetExpression(*op.children[0]);
		if (b_exp->return_type.id() == LogicalTypeId::MAP) {
			function_name = "map_extract";
		} else {
			function_name = "array_extract";
		}
		break;
	}
	case ExpressionType::ARRAY_SLICE:
		function_name = "array_slice";
		break;
	case ExpressionType::STRUCT_EXTRACT: {
		D_ASSERT(op.children.size() == 2);
		D_ASSERT(op.children[0]->expression_class == ExpressionClass::BOUND_EXPRESSION);
		D_ASSERT(op.children[1]->expression_class == ExpressionClass::BOUND_EXPRESSION);
		auto &extract_exp = BoundExpression::GetExpression(*op.children[0]);
		auto &name_exp = BoundExpression::GetExpression(*op.children[1]);
		auto extract_expr_type = extract_exp->return_type.id();
		if (extract_expr_type != LogicalTypeId::STRUCT && extract_expr_type != LogicalTypeId::UNION &&
		    extract_expr_type != LogicalTypeId::SQLNULL) {
			return BindResult(StringUtil::Format(
			    "Cannot extract field %s from expression \"%s\" because it is not a struct or a union",
			    name_exp->ToString(), extract_exp->ToString()));
		}
		function_name = extract_expr_type == LogicalTypeId::UNION ? "union_extract" : "struct_extract";
		break;
	}
	case ExpressionType::ARRAY_CONSTRUCTOR:
		function_name = "list_value";
		break;
	case ExpressionType::ARROW:
		function_name = "json_extract";
		break;
	default:
		break;
	}
	if (!function_name.empty()) {
		auto function = make_uniq_base<ParsedExpression, FunctionExpression>(function_name, std::move(op.children));
		return BindExpression(function, depth, false);
	}

	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < op.children.size(); i++) {
		D_ASSERT(op.children[i]->expression_class == ExpressionClass::BOUND_EXPRESSION);
		children.push_back(std::move(BoundExpression::GetExpression(*op.children[i])));
	}
	// now resolve the types
	LogicalType result_type = ResolveOperatorType(op, children);
	if (op.type == ExpressionType::OPERATOR_COALESCE) {
		if (children.empty()) {
			throw BinderException("COALESCE needs at least one child");
		}
		if (children.size() == 1) {
			return BindResult(std::move(children[0]));
		}
	}

	auto result = make_uniq<BoundOperatorExpression>(op.type, result_type);
	for (auto &child : children) {
		result->children.push_back(std::move(child));
	}
	return BindResult(std::move(result));
}

} // namespace duckdb
