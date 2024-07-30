#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

LogicalType ExpressionBinder::ResolveNotType(OperatorExpression &op, vector<unique_ptr<Expression>> &children) {
	// NOT expression, cast child to BOOLEAN
	D_ASSERT(children.size() == 1);
	children[0] = BoundCastExpression::AddCastToType(context, std::move(children[0]), LogicalType::BOOLEAN);
	return LogicalType(LogicalTypeId::BOOLEAN);
}

LogicalType ExpressionBinder::ResolveCoalesceType(OperatorExpression &op, vector<unique_ptr<Expression>> &children) {
	if (children.empty()) {
		throw InternalException("IN requires at least a single child node");
	}
	// get the maximum type from the children
	LogicalType max_type = ExpressionBinder::GetExpressionReturnType(*children[0]);
	bool is_in_operator = (op.type == ExpressionType::COMPARE_IN || op.type == ExpressionType::COMPARE_NOT_IN);
	for (idx_t i = 1; i < children.size(); i++) {
		auto child_return = ExpressionBinder::GetExpressionReturnType(*children[i]);
		if (is_in_operator) {
			// If it's IN/NOT_IN operator, adjust DECIMAL and VARCHAR returned type.
			if (!BoundComparisonExpression::TryBindComparison(context, max_type, child_return, max_type, op.type)) {
				throw BinderException(op,
				                      "Cannot mix values of type %s and %s in %s clause - an explicit cast is required",
				                      max_type.ToString(), child_return.ToString(),
				                      op.type == ExpressionType::COMPARE_IN ? "IN" : "NOT IN");
			}
		} else {
			// If it's COALESCE operator, don't do extra adjustment.
			if (!LogicalType::TryGetMaxLogicalType(context, max_type, child_return, max_type)) {
				throw BinderException(
				    op, "Cannot mix values of type %s and %s in COALESCE operator - an explicit cast is required",
				    max_type.ToString(), child_return.ToString());
			}
		}
	}

	// cast all children to the same type
	for (auto &child : children) {
		child = BoundCastExpression::AddCastToType(context, std::move(child), max_type);
		if (is_in_operator) {
			// If it's IN/NOT_IN operator, push collation functions.
			ExpressionBinder::PushCollation(context, child, max_type);
		}
	}
	return max_type;
}

LogicalType ExpressionBinder::ResolveOperatorType(OperatorExpression &op, vector<unique_ptr<Expression>> &children) {
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
		ResolveCoalesceType(op, children);
		// (NOT) IN always returns a boolean
		return LogicalType::BOOLEAN;
	case ExpressionType::OPERATOR_COALESCE: {
		return ResolveCoalesceType(op, children);
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

	// Bind the children of the operator expression. We already create bound expressions.
	// Only those children that trigger an error are not yet bound.
	ErrorData error;
	for (idx_t i = 0; i < op.children.size(); i++) {
		BindChild(op.children[i], depth, error);
	}
	if (error.HasError()) {
		return BindResult(std::move(error));
	}

	// all children bound successfully
	string function_name;
	switch (op.type) {
	case ExpressionType::ARRAY_EXTRACT: {
		D_ASSERT(op.children[0]->expression_class == ExpressionClass::BOUND_EXPRESSION);
		auto &b_exp = BoundExpression::GetExpression(*op.children[0]);
		const auto &b_exp_type = b_exp->return_type;
		if (b_exp_type.id() == LogicalTypeId::MAP) {
			function_name = "map_extract";
		} else if (b_exp_type.IsJSONType() && op.children.size() == 2) {
			function_name = "json_extract";
			// Make sure we only extract array elements, not fields, by adding the $[] syntax
			auto &i_exp = BoundExpression::GetExpression(*op.children[1]);
			if (i_exp->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				auto &const_exp = i_exp->Cast<BoundConstantExpression>();
				if (!const_exp.value.IsNull()) {
					const_exp.value = StringUtil::Format("$[%s]", const_exp.value.ToString());
					const_exp.return_type = LogicalType::VARCHAR;
				}
			}
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
		const auto &extract_expr_type = extract_exp->return_type;
		if (extract_expr_type.id() != LogicalTypeId::STRUCT && extract_expr_type.id() != LogicalTypeId::UNION &&
		    extract_expr_type.id() != LogicalTypeId::SQLNULL && !extract_expr_type.IsJSONType()) {
			return BindResult(StringUtil::Format(
			    "Cannot extract field %s from expression \"%s\" because it is not a struct, union, or json",
			    name_exp->ToString(), extract_exp->ToString()));
		}
		if (extract_expr_type.id() == LogicalTypeId::UNION) {
			function_name = "union_extract";
		} else if (extract_expr_type.IsJSONType()) {
			function_name = "json_extract";
			// Make sure we only extract fields, not array elements, by adding $. syntax
			if (name_exp->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				auto &const_exp = name_exp->Cast<BoundConstantExpression>();
				if (!const_exp.value.IsNull()) {
					const_exp.value = StringUtil::Format("$.\"%s\"", const_exp.value.ToString());
					const_exp.return_type = LogicalType::VARCHAR;
				}
			}
		} else {
			function_name = "struct_extract";
		}
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
