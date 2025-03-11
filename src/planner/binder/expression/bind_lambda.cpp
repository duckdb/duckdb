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

void ExtractParameter(ParsedExpression &expr, vector<string> &column_names, vector<string> &column_aliases) {

	auto &column_ref = expr.Cast<ColumnRefExpression>();
	if (column_ref.IsQualified()) {
		throw BinderException(LambdaExpression::InvalidParametersErrorMessage());
	}

	column_names.push_back(column_ref.GetName());
	column_aliases.push_back(column_ref.ToString());
}

void ExtractParameters(LambdaExpression &expr, vector<string> &column_names, vector<string> &column_aliases) {

	// extract the lambda parameters, which are a single column
	// reference, or a list of column references (ROW function)
	string error_message;
	auto column_refs = expr.ExtractColumnRefExpressions(error_message);
	if (!error_message.empty()) {
		throw BinderException(error_message);
	}

	for (const auto &column_ref : column_refs) {
		ExtractParameter(column_ref.get(), column_names, column_aliases);
	}
	D_ASSERT(!column_names.empty());
}

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, const LogicalType &list_child_type,
                                            optional_ptr<bind_lambda_function_t> bind_lambda_function) {

	// This is not a lambda expression, but the JSON arrow operator.
	if (!bind_lambda_function) {
		OperatorExpression arrow_expr(ExpressionType::ARROW, std::move(expr.lhs), std::move(expr.expr));
		auto bind_result = BindExpression(arrow_expr, depth);

		// The arrow_expr now contains bound nodes. We move these into the original expression.
		if (bind_result.HasError()) {
			D_ASSERT(arrow_expr.children.size() == 2);
			expr.lhs = std::move(arrow_expr.children[0]);
			expr.expr = std::move(arrow_expr.children[1]);
		}
		return bind_result;
	}

	// extract and verify lambda parameters to create dummy columns
	vector<LogicalType> column_types;
	vector<string> column_names;
	vector<string> column_aliases;
	ExtractParameters(expr, column_names, column_aliases);
	for (idx_t i = 0; i < column_names.size(); i++) {
		column_types.push_back((*bind_lambda_function)(i, list_child_type));
	}

	// base table alias
	auto table_alias = StringUtil::Join(column_aliases, ", ");
	if (column_aliases.size() > 1) {
		table_alias = "(" + table_alias + ")";
	}

	// create a lambda binding and push it to the lambda bindings vector
	vector<DummyBinding> local_bindings;
	if (!lambda_bindings) {
		lambda_bindings = &local_bindings;
	}
	DummyBinding new_lambda_binding(column_types, column_names, table_alias);
	lambda_bindings->push_back(new_lambda_binding);

	auto result = BindExpression(expr.expr, depth, false);
	lambda_bindings->pop_back();

	// successfully bound a subtree of nested lambdas, set this to nullptr in case other parts of the
	// query also contain lambdas
	if (lambda_bindings->empty()) {
		lambda_bindings = nullptr;
	}

	if (result.HasError()) {
		result.error.Throw();
	}

	return BindResult(make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::LAMBDA,
	                                                   std::move(result.expression), column_names.size()));
}

void ExpressionBinder::TransformCapturedLambdaColumn(unique_ptr<Expression> &original,
                                                     unique_ptr<Expression> &replacement,
                                                     BoundLambdaExpression &bound_lambda_expr,
                                                     const optional_ptr<bind_lambda_function_t> bind_lambda_function,
                                                     const LogicalType &list_child_type) {

	// check if the original expression is a lambda parameter
	if (original->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA_REF) {

		auto &bound_lambda_ref = original->Cast<BoundLambdaRefExpression>();
		auto alias = bound_lambda_ref.GetAlias();

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

	replacement = make_uniq<BoundReferenceExpression>(original->GetAlias(), original->return_type, offset);
	bound_lambda_expr.captures.push_back(std::move(original));
}

void ExpressionBinder::CaptureLambdaColumns(BoundLambdaExpression &bound_lambda_expr, unique_ptr<Expression> &expr,
                                            const optional_ptr<bind_lambda_function_t> bind_lambda_function,
                                            const LogicalType &list_child_type) {

	if (expr->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		throw BinderException("subqueries in lambda expressions are not supported");
	}

	// these are bound depth-first
	D_ASSERT(expr->GetExpressionClass() != ExpressionClass::BOUND_LAMBDA);

	// we do not need to replace anything, as these will be constant in the lambda expression
	// when executed by the expression executor
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return;
	}

	// these expression classes do not have children, transform them
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
	    expr->GetExpressionClass() == ExpressionClass::BOUND_PARAMETER ||
	    expr->GetExpressionClass() == ExpressionClass::BOUND_LAMBDA_REF) {

		if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			// Search for UNNEST.
			auto &column_binding = expr->Cast<BoundColumnRefExpression>().binding;
			ThrowIfUnnestInLambda(column_binding);
		}

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
