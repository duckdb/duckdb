#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, const bool is_lambda,
                                            const LogicalType &list_child_type) {

	if (!is_lambda) {
		// this is for binding JSON
		auto lhs_expr = expr.lhs->Copy();
		OperatorExpression arrow_expr(ExpressionType::ARROW, move(lhs_expr), expr.expr->Copy());
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
		expr.params.push_back(move(expr.lhs));
	} else {
		auto &func_expr = (FunctionExpression &)*expr.lhs;
		for (idx_t i = 0; i < func_expr.children.size(); i++) {
			expr.params.push_back(move(func_expr.children[i]));
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

		auto column_ref = (ColumnRefExpression &)*expr.params[i];
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
		auto result = BindExpression(&expr.params[i], depth, false);
		D_ASSERT(!result.HasError());
	}

	auto result = BindExpression(&expr.expr, depth, false);
	lambda_bindings->pop_back();

	// successfully bound a subtree of nested lambdas, set this to nullptr in case other parts of the
	// query also contain lambdas
	if (lambda_bindings->empty()) {
		lambda_bindings = nullptr;
	}

	if (result.HasError()) {
		throw BinderException(result.error);
	}

	return BindResult(make_unique<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::LAMBDA,
	                                                     move(result.expression), params_strings.size()));
}

void ExpressionBinder::TransformCapturedLambdaColumn(unique_ptr<Expression> &original,
                                                     unique_ptr<Expression> &replacement,
                                                     vector<unique_ptr<Expression>> &captures,
                                                     LogicalType &list_child_type, string &alias) {

	// check if the original expression is a lambda parameter
	bool is_lambda_parameter = false;
	if (original->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

		// determine if this is the lambda parameter
		auto &bound_col_ref = (BoundColumnRefExpression &)*original;
		if (bound_col_ref.binding.table_index == DConstants::INVALID_INDEX) {
			is_lambda_parameter = true;
		}
	}

	if (is_lambda_parameter) {
		// this is a lambda parameter, so the replacement refers to the first argument, which is the list
		replacement = make_unique<BoundReferenceExpression>(alias, list_child_type, 0);

	} else {
		// this is not a lambda parameter, so we need to create a new argument for the arguments vector
		replacement =
		    make_unique<BoundReferenceExpression>(original->alias, original->return_type, captures.size() + 1);
		captures.push_back(move(original));
	}
}

void ExpressionBinder::CaptureLambdaColumns(vector<unique_ptr<Expression>> &captures, LogicalType &list_child_type,
                                            unique_ptr<Expression> &expr, string &alias) {

	if (expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
		throw InvalidInputException("Subqueries are not supported in lambda expressions!");
	}

	// these expression classes do not have children, transform them
	if (expr->expression_class == ExpressionClass::BOUND_CONSTANT ||
	    expr->expression_class == ExpressionClass::BOUND_COLUMN_REF ||
	    expr->expression_class == ExpressionClass::BOUND_PARAMETER) {

		// move the expr because we are going to replace it
		auto original = move(expr);
		unique_ptr<Expression> replacement;

		TransformCapturedLambdaColumn(original, replacement, captures, list_child_type, alias);

		// replace the expression
		expr = move(replacement);

	} else {
		// recursively enumerate the children of the expression
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			CaptureLambdaColumns(captures, list_child_type, child, alias);
		});
	}
}

} // namespace duckdb
