#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

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
	bool is_in_operator = (op.GetExpressionType() == ExpressionType::COMPARE_IN ||
	                       op.GetExpressionType() == ExpressionType::COMPARE_NOT_IN);
	for (idx_t i = 1; i < children.size(); i++) {
		auto child_return = ExpressionBinder::GetExpressionReturnType(*children[i]);
		if (is_in_operator) {
			// If it's IN/NOT_IN operator, adjust DECIMAL and VARCHAR returned type.
			if (!BoundComparisonExpression::TryBindComparison(context, max_type, child_return, max_type,
			                                                  op.GetExpressionType())) {
				throw BinderException(op,
				                      "Cannot mix values of type %s and %s in %s clause - an explicit cast is required",
				                      max_type.ToString(), child_return.ToString(),
				                      op.GetExpressionType() == ExpressionType::COMPARE_IN ? "IN" : "NOT IN");
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
	switch (op.GetExpressionType()) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		// IS (NOT) NULL always returns a boolean, and does not cast its children
		if (!children[0]->GetReturnType().IsValid()) {
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
	case ExpressionType::OPERATOR_TRY: {
		return children[0]->GetReturnType();
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

// Bind (expr).field
static BindResult BindFieldExpression(Binder &binder, OperatorExpression &op) {
	D_ASSERT(op.GetChildren().size() == 2);
	D_ASSERT(op.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
	D_ASSERT(op.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);

	vector<unique_ptr<Expression>> children;
	for (auto &child : op.GetChildrenMutable()) {
		children.push_back(std::move(BoundExpression::GetExpression(*child)));
	}

	const auto value_type = children[0]->GetReturnType();
	const auto field_name = children[1]->ToString();

	const Identifier schema(DEFAULT_SCHEMA);
	Identifier name("field");

	if (value_type.IsJSONType()) {
		// JSON field access: rewrite a constant key into a `$."x"` path and dispatch to json_extract
		name = Identifier("json_extract");
		if (children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &const_exp = children[1]->Cast<BoundConstantExpression>();
			if (!const_exp.GetValue().IsNull()) {
				const_exp.GetValueMutable() = Value(StringUtil::Format("$.\"%s\"", const_exp.GetValue().ToString()));
				const_exp.SetReturnType(LogicalType::VARCHAR);
			}
		}
	}

	FunctionBinder function_binder(binder);

	ErrorData error;
	auto expr = function_binder.BindScalarFunction(schema, name, std::move(children), error);
	if (error.HasError()) {
		return BindResult(
		    BinderException(op, "Cannot get field %s from expression of type '%s'", field_name, value_type.ToString()));
	}

	return BindResult(std::move(expr));
}

// Bind (expr)[n]
static BindResult BindElementExpression(Binder &binder, OperatorExpression &op) {
	D_ASSERT(op.GetChildren().size() == 2);
	D_ASSERT(op.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
	D_ASSERT(op.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);

	vector<unique_ptr<Expression>> children;
	for (auto &child : op.GetChildrenMutable()) {
		children.push_back(std::move(BoundExpression::GetExpression(*child)));
	}

	const auto value_type = children[0]->GetReturnType();
	const auto field_name = children[1]->ToString();
	const auto field_text = children[1]->GetReturnType().IsIntegral()
	                            ? StringUtil::Format("element '%s'", field_name)
	                            : StringUtil::Format("element with key '%s'", field_name);

	const Identifier schema(DEFAULT_SCHEMA);
	Identifier name("element");

	if (value_type.IsJSONType()) {
		// JSON subscript: rewrite a constant integer index into `$[i]` or a string key into `$."x"`, then dispatch
		// to json_extract
		name = Identifier("json_extract");
		if (children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &const_exp = children[1]->Cast<BoundConstantExpression>();
			if (!const_exp.GetValue().IsNull()) {
				if (const_exp.GetValueMutable().TryCastAs(binder.context, LogicalType::UINTEGER)) {
					// array element: if the cast fails it is out-of-bounds for a JSON array
					auto index = UIntegerValue::Get(const_exp.GetValue());
					const_exp.GetValueMutable() = Value(StringUtil::Format("$[%llu]", index));
					const_exp.SetReturnType(LogicalType::VARCHAR);
				} else if (const_exp.GetReturnType().id() == LogicalTypeId::VARCHAR) {
					// field access
					const_exp.GetValueMutable() =
					    Value(StringUtil::Format("$.\"%s\"", const_exp.GetValue().ToString()));
					const_exp.SetReturnType(LogicalType::VARCHAR);
				}
			}
		}
	} else if (value_type.id() == LogicalTypeId::VARIANT) {
		// VARIANT subscript: a numeric index must be cast to UINTEGER to match variant_extract's array overload
		if (children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &const_exp = children[1]->Cast<BoundConstantExpression>();
			if (!const_exp.GetValue().IsNull() && const_exp.GetReturnType().IsNumeric()) {
				const_exp.GetValueMutable() = const_exp.GetValueMutable().DefaultCastAs(LogicalType::UINTEGER, true);
				const_exp.SetReturnType(LogicalType::UINTEGER);
			}
		}
	}

	FunctionBinder function_binder(binder);

	ErrorData error;
	auto expr = function_binder.BindScalarFunction(schema, name, std::move(children), error);
	if (error.HasError()) {
		return BindResult(
		    BinderException(op, "Cannot get %s from expression of type '%s'", field_text, value_type.ToString()));
	}

	return BindResult(std::move(expr));
}

// Bind (expr)[m:n:s]
static BindResult BindSliceExpression(Binder &binder, OperatorExpression &op) {
	D_ASSERT(op.GetChildren().size() >= 2);
	D_ASSERT(op.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
	D_ASSERT(op.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);

	vector<unique_ptr<Expression>> children;
	for (auto &child : op.GetChildrenMutable()) {
		children.push_back(std::move(BoundExpression::GetExpression(*child)));
	}

	const auto value_type = children[0]->GetReturnType();

	const Identifier schema(DEFAULT_SCHEMA);
	const Identifier name("slice");

	FunctionBinder function_binder(binder);

	ErrorData error;
	auto expr = function_binder.BindScalarFunction(schema, name, std::move(children), error);
	if (error.HasError()) {
		return BindResult(
		    BinderException(op, "Cannot slice expression of type '%s'\n%s", value_type.ToString(), error.RawMessage()));
	}

	return BindResult(std::move(expr));
}

BindResult ExpressionBinder::BindExpression(OperatorExpression &op, idx_t depth) {
	auto operator_type = op.GetExpressionType();
	if (operator_type == ExpressionType::GROUPING_FUNCTION) {
		return BindGroupingFunction(op, depth);
	}

	// Bind the children of the operator expression. We already create bound expressions.
	// Only those children that trigger an error are not yet bound.
	ErrorData error;
	if (operator_type == ExpressionType::OPERATOR_TRY) {
		D_ASSERT(op.GetChildrenMutable().size() == 1);
		inside_try = true;
		BindChild(op.GetChildrenMutable()[0], depth, error);
		inside_try = false;
	} else {
		for (idx_t i = 0; i < op.GetChildrenMutable().size(); i++) {
			BindChild(op.GetChildrenMutable()[i], depth, error);
		}
	}

	if (error.HasError()) {
		return BindResult(std::move(error));
	}

	// all children bound successfully
	string function_name;
	switch (op.GetExpressionType()) {
	case ExpressionType::OPERATOR_UNPACK:
		return BindResult("UNPACK not allowed here, should have been resolved earlier");
	case ExpressionType::ARRAY_EXTRACT:
		return BindElementExpression(GetBinder(), op);
	case ExpressionType::ARRAY_SLICE:
		return BindSliceExpression(GetBinder(), op);
	case ExpressionType::STRUCT_EXTRACT:
		return BindFieldExpression(GetBinder(), op);
	case ExpressionType::ARRAY_CONSTRUCTOR:
		function_name = "list_value";
		break;
	case ExpressionType::ARROW:
		function_name = "json_extract";
		break;
	case ExpressionType::OPERATOR_TRY: {
		auto &expr = BoundExpression::GetExpression(*op.GetChildrenMutable()[0]);
		if (expr->HasSubquery()) {
			throw BinderException("TRY can not be used in combination with a scalar subquery");
		}
		if (expr->IsVolatile()) {
			throw BinderException("TRY can not be used in combination with a volatile function");
		}
		break;
	}
	default:
		break;
	}
	if (!function_name.empty()) {
		auto function = make_uniq_base<ParsedExpression, FunctionExpression>(Identifier(function_name),
		                                                                     std::move(op.GetChildrenMutable()));
		return BindExpression(function, depth, false);
	}

	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < op.GetChildrenMutable().size(); i++) {
		D_ASSERT(op.GetChildrenMutable()[i]->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
		children.push_back(std::move(BoundExpression::GetExpression(*op.GetChildrenMutable()[i])));
	}
	// now resolve the types
	LogicalType result_type = ResolveOperatorType(op, children);
	if (op.GetExpressionType() == ExpressionType::OPERATOR_COALESCE) {
		if (children.empty()) {
			throw BinderException("COALESCE needs at least one child");
		}
		if (children.size() == 1) {
			return BindResult(std::move(children[0]));
		}
	}

	auto result = make_uniq<BoundOperatorExpression>(op.GetExpressionType(), result_type);
	for (auto &child : children) {
		result->GetChildrenMutable().push_back(std::move(child));
	}
	return BindResult(std::move(result));
}

} // namespace duckdb
