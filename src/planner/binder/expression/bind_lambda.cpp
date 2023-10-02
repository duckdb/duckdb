#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, const bool is_lambda,
                                            const LogicalType &list_child_type) {

	if (!is_lambda) {
		// this is for binding JSON
		auto lhs_expr = expr.lhs->Copy();
		OperatorExpression arrow_expr(ExpressionType::ARROW, std::move(lhs_expr), expr.expr->Copy());
		return BindExpression(arrow_expr, depth);
	}

	// binding the lambda expression
	D_ASSERT(expr.lhs);
	if (expr.lhs->expression_class != ExpressionClass::FUNCTION &&
	    expr.lhs->expression_class != ExpressionClass::COLUMN_REF) {
		throw BinderException(
		    "Invalid parameter list! Parameters must be comma-separated column names, e.g. x or (x, y).");
	}

	// move the lambda parameters to the params vector
	if (expr.lhs->expression_class == ExpressionClass::COLUMN_REF) {
		expr.params.push_back(std::move(expr.lhs));
	} else {
		auto &func_expr = expr.lhs->Cast<FunctionExpression>();
		for (idx_t i = 0; i < func_expr.children.size(); i++) {
			expr.params.push_back(std::move(func_expr.children[i]));
		}
	}
	D_ASSERT(!expr.params.empty());

	// create dummy columns for the lambda parameters (lhs)
	vector<LogicalType> column_types;
	vector<string> column_names;
	vector<string> params_strings;

	// positional parameters as column references
	for (idx_t i = 0; i < expr.params.size(); i++) {
		if (expr.params[i]->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			throw BinderException("Parameter must be a column name.");
		}

		auto column_ref = expr.params[i]->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			throw BinderException("Invalid parameter name '%s': must be unqualified", column_ref.ToString());
		}

		column_types.emplace_back(list_child_type);
		column_names.push_back(column_ref.GetColumnName());
		params_strings.push_back(expr.params[i]->ToString());
	}

	// base table alias
	auto params_alias = StringUtil::Join(params_strings, ", ");
	if (params_strings.size() > 1) {
		params_alias = "(" + params_alias + ")";
	}

	// create a lambda binding and push it to the lambda bindings vector
	vector<DummyBinding> local_bindings;
	if (!lambda_bindings) {
		lambda_bindings = &local_bindings;
	}
	DummyBinding new_lambda_binding(column_types, column_names, params_alias);
	lambda_bindings->push_back(new_lambda_binding);

	// bind the parameter expressions
	for (idx_t i = 0; i < expr.params.size(); i++) {
		auto result = BindExpression(expr.params[i], depth, false);
		if (result.HasError()) {
			throw InternalException("Error during lambda binding: %s", result.error);
		}
	}

	auto result = BindExpression(expr.expr, depth, false);
	lambda_bindings->pop_back();

	// successfully bound a subtree of nested lambdas, set this to nullptr in case other parts of the
	// query also contain lambdas
	if (lambda_bindings->empty()) {
		lambda_bindings = nullptr;
	}

	if (result.HasError()) {
		throw BinderException(result.error);
	}

	return BindResult(make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::LAMBDA,
	                                                   std::move(result.expression), params_strings.size()));
}

void ExpressionBinder::TransformCapturedLambdaColumn(unique_ptr<Expression> &original,
                                                     unique_ptr<Expression> &replacement,
                                                     vector<unique_ptr<Expression>> &captures,
                                                     LogicalType &list_child_type) {

	// check if the original expression is a lambda parameter
	if (original->expression_class == ExpressionClass::BOUND_LAMBDA_REF) {

		// determine if this is the lambda parameter
		auto &bound_lambda_ref = original->Cast<BoundLambdaRefExpression>();
		auto alias = bound_lambda_ref.alias;

		if (lambda_bindings && bound_lambda_ref.lambda_index != lambda_bindings->size()) {

			D_ASSERT(bound_lambda_ref.lambda_index < lambda_bindings->size());
			auto &lambda_binding = (*lambda_bindings)[bound_lambda_ref.lambda_index];

			D_ASSERT(lambda_binding.names.size() == 1);
			D_ASSERT(lambda_binding.types.size() == 1);
			// refers to a lambda parameter outside of the current lambda function
			replacement =
			    make_uniq<BoundReferenceExpression>(lambda_binding.names[0], lambda_binding.types[0],
			                                        lambda_bindings->size() - bound_lambda_ref.lambda_index + 1);

		} else {
			// refers to current lambda parameter
			replacement = make_uniq<BoundReferenceExpression>(alias, list_child_type, 0);
		}

	} else {
		// always at least the current lambda parameter
		idx_t index_offset = 1;
		if (lambda_bindings) {
			index_offset += lambda_bindings->size();
		}

		// this is not a lambda parameter, so we need to create a new argument for the arguments vector
		replacement = make_uniq<BoundReferenceExpression>(original->alias, original->return_type,
		                                                  captures.size() + index_offset + 1);
		captures.push_back(std::move(original));
	}
}

void ExpressionBinder::CaptureLambdaColumns(vector<unique_ptr<Expression>> &captures, LogicalType &list_child_type,
                                            unique_ptr<Expression> &expr) {

	if (expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
		throw InvalidInputException("Subqueries are not supported in lambda expressions!");
	}

	// these expression classes do not have children, transform them
	if (expr->expression_class == ExpressionClass::BOUND_CONSTANT ||
	    expr->expression_class == ExpressionClass::BOUND_COLUMN_REF ||
	    expr->expression_class == ExpressionClass::BOUND_PARAMETER ||
	    expr->expression_class == ExpressionClass::BOUND_LAMBDA_REF) {

		// move the expr because we are going to replace it
		auto original = std::move(expr);
		unique_ptr<Expression> replacement;

		TransformCapturedLambdaColumn(original, replacement, captures, list_child_type);

		// replace the expression
		expr = std::move(replacement);

	} else {
		// recursively enumerate the children of the expression
		ExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<Expression> &child) { CaptureLambdaColumns(captures, list_child_type, child); });
	}

	expr->Verify();
}

} // namespace duckdb
