#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_binding.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

//! Bind arms in their original SQL scope. Only their inputs cross into the generated lambda.
static void CaptureCaseInput(unique_ptr<Expression> &input, vector<unique_ptr<Expression>> &captures) {
	switch (input->GetExpressionClass()) {
	case ExpressionClass::BOUND_SUBQUERY:
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_LAMBDA_REF:
		break;
	case ExpressionClass::BOUND_LAMBDA:
		// Its captures are children of the enclosing function.
		return;
	default:
		ExpressionIterator::EnumerateChildren(
		    *input, [&](unique_ptr<Expression> &child) { CaptureCaseInput(child, captures); });
		return;
	}
	auto reference =
	    make_uniq<BoundReferenceExpression>(input->GetAlias(), input->GetReturnType(), 1 + captures.size());
	captures.push_back(std::move(input));
	input = std::move(reference);
}

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	ErrorData error;
	unique_ptr<Expression> operand;
	bool volatile_operand = false;
	bool share_operand = false;
	vector<unique_ptr<Expression>> captures;
	vector<pair<unique_ptr<Expression>, unique_ptr<Expression>>> checks;
	if (expr.CaseOperand()) {
		operand = BindChild(expr.CaseOperandMutable(), depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}
		volatile_operand = operand->IsVolatile();
		// Even without a volatile scalar call, a subquery can sample or choose an unordered row.
		share_operand = volatile_operand || operand->HasSubquery();
	}
	for (auto &arm : expr.SimpleChecksMutable()) {
		vector<unique_ptr<Expression>> conditions;
		for (auto &test : arm.when) {
			auto rhs = BindChild(test.expression, depth, error);
			if (!rhs) {
				return BindResult(std::move(error));
			}
			unique_ptr<Expression> lhs;
			if (share_operand) {
				lhs = make_uniq<BoundReferenceExpression>(operand->GetName(), operand->GetReturnType(), 0);
			} else {
				lhs = operand->Copy();
			}
			auto comparison = CreateBoundComparison(test.comparison_type, std::move(lhs), std::move(rhs), error);
			if (!comparison) {
				error.AddQueryLocation(expr);
				return BindResult(std::move(error));
			}
			if (share_operand) {
				// Comparison binding assigns a type to an untyped parameter before it becomes a lambda input.
				CaptureCaseInput(BoundComparisonExpression::RightMutable(comparison->Cast<BoundFunctionExpression>()),
				                 captures);
			}
			conditions.push_back(std::move(comparison));
		}
		auto condition = BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(conditions));
		checks.emplace_back(std::move(condition), BindChild(arm.then_expr, depth, error));
	}
	for (auto &check : expr.CaseChecksMutable()) {
		auto when_expr = BindChild(check.when_expr, depth, error);
		auto then_expr = BindChild(check.then_expr, depth, error);
		checks.emplace_back(std::move(when_expr), std::move(then_expr));
	}
	auto else_expr = BindChild(expr.ElseMutable(), depth, error);
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	// the children have been successfully resolved
	// figure out the result type of the CASE expression
	auto return_type = ExpressionBinder::GetExpressionReturnType(*else_expr);
	const auto decimal_width_limit = binder.GetDecimalWidthLimit();
	for (auto &check : checks) {
		auto then_type = ExpressionBinder::GetExpressionReturnType(*check.second);
		if (!LogicalType::TryGetMaxLogicalType(context, return_type, then_type, return_type, decimal_width_limit)) {
			throw BinderException(
			    expr, "Cannot mix values of type %s and %s in CASE expression - an explicit cast is required",
			    return_type.ToString(), then_type.ToString());
		}
	}

	// bind all the individual components of the CASE statement
	auto result = make_uniq<BoundCaseExpression>(return_type);
	for (auto &check : checks) {
		BoundCaseCheck result_check;
		result_check.when_expr =
		    BoundCastExpression::AddCastToType(context, std::move(check.first), LogicalType::BOOLEAN);
		result_check.then_expr = BoundCastExpression::AddCastToType(context, std::move(check.second), return_type);
		result->CaseChecksMutable().push_back(std::move(result_check));
	}
	result->ElseMutable() = BoundCastExpression::AddCastToType(context, std::move(else_expr), return_type);
	if (!share_operand) {
		return BindResult(std::move(result));
	}

	for (auto &check : result->CaseChecksMutable()) {
		CaptureCaseInput(check.then_expr, captures);
	}
	CaptureCaseInput(result->ElseMutable(), captures);

	Identifier parameter_name("__case_operand");
	if (lambda_bindings) {
		identifier_set_t outer_parameters;
		for (auto &binding : *lambda_bindings) {
			for (auto &name : binding.GetColumnNames()) {
				outer_parameters.insert(name);
			}
		}
		for (idx_t suffix = 1; outer_parameters.count(parameter_name); suffix++) {
			parameter_name = Identifier("__case_operand_" + to_string(suffix));
		}
	}
	auto lambda = make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::LAMBDA, std::move(result), 1);
	lambda->SetParameterNames({parameter_name});
	lambda->inline_after_subqueries = !volatile_operand;
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(lambda));
	arguments.push_back(std::move(operand));
	auto invoke = FunctionBinder(binder).BindScalarFunction(DEFAULT_SCHEMA, "invoke", std::move(arguments), false,
	                                                        &binder, expr.GetAlias(), expr.GetQueryLocation());
	for (auto &capture : captures) {
		if (volatile_operand && capture->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			// An eager invoke argument would execute even if the optimizer could discard the arm.
			throw BinderException(expr, "A simple CASE with a volatile operand and multiple comparisons cannot "
			                            "contain a subquery in a branch");
		}
		invoke->Cast<BoundFunctionExpression>().GetChildrenMutable().push_back(std::move(capture));
	}
	return BindResult(std::move(invoke));
}
} // namespace duckdb
