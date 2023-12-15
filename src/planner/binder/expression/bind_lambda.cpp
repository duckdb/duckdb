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

idx_t GetLambdaParamCount(const vector<DummyBinding> &lambda_bindings) {
	idx_t count = 0;
	for (auto &binding : lambda_bindings) {
		count += binding.names.size();
	}
	return count;
}

idx_t GetLambdaParamIndex(const vector<DummyBinding> &lambda_bindings, const BoundLambdaExpression &bound_lambda_expr,
                          const BoundLambdaRefExpression &bound_lambda_ref_expr) {
	D_ASSERT(bound_lambda_ref_expr.lambda_idx < lambda_bindings.size());
	idx_t offset = 0;
	// count the remaining lambda parameters BEFORE the current lambda parameter,
	// as these will be in front of the current lambda parameter in the input chunk
	for (idx_t i = bound_lambda_ref_expr.lambda_idx + 1; i < lambda_bindings.size(); i++) {
		offset += lambda_bindings[i].names.size();
	}
	offset +=
	    lambda_bindings[bound_lambda_ref_expr.lambda_idx].names.size() - bound_lambda_ref_expr.binding.column_index - 1;
	offset += bound_lambda_expr.parameter_count;
	return offset;
}

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, const LogicalType &list_child_type,
                                            optional_ptr<bind_lambda_function_t> bind_lambda_function) {

	// this is for binding JSON
	if (!bind_lambda_function) {
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
			throw BinderException("Lambda parameter must be a column name.");
		}

		auto column_ref = expr.params[i]->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			throw BinderException("Invalid lambda parameter name '%s': must be unqualified", column_ref.ToString());
		}

		column_types.push_back((*bind_lambda_function)(i, list_child_type));
		column_names.push_back(column_ref.GetColumnName());
		params_strings.push_back(expr.params[i]->ToString());
	}

	// ensure that we do not have ambiguous lambda parameters
	if (lambda_bindings) {
		for (const auto &binding : *lambda_bindings) {
			for (const auto &outer_lambda_parameter : binding.names) {
				for (const auto &this_lambda_parameter : column_names) {
					if (outer_lambda_parameter == this_lambda_parameter) {
						throw BinderException("Ambiguous lambda parameter name: '%s'. Try changing your lambda "
						                      "parameter name. \n Some list functions use lambda functions "
						                      "under the hood, so *the same* function cannot be nested, like "
						                      "list_intersect(list_intersect(...),...), list_has_any, list_has_all, "
						                      "and their aliases. \n "
						                      "Try writing them out manually with lambda functions to define explicit "
						                      "lambda parameter names.",
						                      outer_lambda_parameter);
					}
				}
			}
		}
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
                                                     BoundLambdaExpression &bound_lambda_expr,
                                                     const optional_ptr<bind_lambda_function_t> bind_lambda_function,
                                                     const LogicalType &list_child_type) {

	// check if the original expression is a lambda parameter
	if (original->expression_class == ExpressionClass::BOUND_LAMBDA_REF) {

		auto &bound_lambda_ref = original->Cast<BoundLambdaRefExpression>();
		auto alias = bound_lambda_ref.alias;

		// refers to a lambda parameter outside the current lambda function
		// so the lambda parameter will be inside the lambda_bindings
		if (lambda_bindings && bound_lambda_ref.lambda_idx != lambda_bindings->size()) {

			auto &binding = (*lambda_bindings)[bound_lambda_ref.lambda_idx];
			D_ASSERT(binding.names.size() == binding.types.size());

			// find the matching dummy column in the lambda binding
			for (idx_t column_idx = 0; column_idx < binding.names.size(); column_idx++) {
				if (column_idx == bound_lambda_ref.binding.column_index) {

					// now create the replacement
					auto index = GetLambdaParamIndex(*lambda_bindings, bound_lambda_expr, bound_lambda_ref);
					replacement = make_uniq<BoundReferenceExpression>(binding.names[column_idx],
					                                                  binding.types[column_idx], index);
					return;
				}
			}

			// error resolving the lambda index
			throw InternalException("Failed to bind lambda parameter internally");
		}

		// refers to a lambda parameter inside the current lambda function
		auto logical_type = (*bind_lambda_function)(bound_lambda_ref.binding.column_index, list_child_type);
		auto index = bound_lambda_expr.parameter_count - bound_lambda_ref.binding.column_index - 1;
		replacement = make_uniq<BoundReferenceExpression>(alias, logical_type, index);
		return;
	}

	// this is not a lambda parameter, get the capture offset
	idx_t offset = 0;
	if (lambda_bindings) {
		offset += GetLambdaParamCount(*lambda_bindings);
	}
	offset += bound_lambda_expr.parameter_count;
	offset += bound_lambda_expr.captures.size();

	replacement = make_uniq<BoundReferenceExpression>(original->alias, original->return_type, offset);
	bound_lambda_expr.captures.push_back(std::move(original));
}

void ExpressionBinder::CaptureLambdaColumns(BoundLambdaExpression &bound_lambda_expr, unique_ptr<Expression> &expr,
                                            const optional_ptr<bind_lambda_function_t> bind_lambda_function,
                                            const LogicalType &list_child_type) {

	if (expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
		throw InvalidInputException("Subqueries are not supported in lambda expressions!");
	}

	// these are bound depth-first
	D_ASSERT(expr->expression_class != ExpressionClass::BOUND_LAMBDA);

	// we do not need to replace anything, as these will be constant in the lambda expression
	// when executed by the expression executor
	if (expr->expression_class == ExpressionClass::BOUND_CONSTANT) {
		return;
	}

	// these expression classes do not have children, transform them
	if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF ||
	    expr->expression_class == ExpressionClass::BOUND_PARAMETER ||
	    expr->expression_class == ExpressionClass::BOUND_LAMBDA_REF) {

		// move the expr because we are going to replace it
		auto original = std::move(expr);
		unique_ptr<Expression> replacement;

		TransformCapturedLambdaColumn(original, replacement, bound_lambda_expr, bind_lambda_function, list_child_type);

		// replace the expression
		expr = std::move(replacement);

	} else {
		// recursively enumerate the children of the expression
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			CaptureLambdaColumns(bound_lambda_expr, child, bind_lambda_function, list_child_type);
		});
	}

	expr->Verify();
}

} // namespace duckdb
