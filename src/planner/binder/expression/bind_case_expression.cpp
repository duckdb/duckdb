#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

static unique_ptr<CaseExpression> CreateBoundLegacyCaseExpression(const CaseExpression &expr,
                                                                  const Expression &case_operand,
                                                                  QueryLocation operand_location) {
	vector<unique_ptr<ParsedExpression>> case_operands;
	case_operands.reserve(expr.CaseChecks().size());
	for (idx_t i = 0; i < expr.CaseChecks().size(); i++) {
		auto operand = make_uniq<BoundExpression>(case_operand.Copy());
		operand->SetQueryLocation(operand_location);
		case_operands.push_back(std::move(operand));
	}
	return expr.GetLegacyCaseExpression(std::move(case_operands));
}

static unique_ptr<ParsedExpression> CreateCaseInvokeExpression(const CaseExpression &expr, idx_t lambda_index,
                                                               Identifier parameter_name,
                                                               QueryLocation operand_location,
                                                               unique_ptr<ParsedExpression> case_operand) {
	auto case_body = make_uniq<CaseExpression>();
	for (auto &check : expr.CaseChecks()) {
		CaseCheck invoke_check;
		auto lambda_ref = make_uniq<LambdaRefExpression>(lambda_index, parameter_name);
		lambda_ref->SetQueryLocation(operand_location);
		invoke_check.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(lambda_ref),
		                                                         check.when_expr->Copy());
		invoke_check.when_expr->SetQueryLocation(operand_location);
		invoke_check.then_expr = check.then_expr->Copy();
		case_body->CaseChecksMutable().push_back(std::move(invoke_check));
	}
	case_body->ElseMutable() = expr.Else().Copy();

	vector<string> parameters;
	parameters.push_back(parameter_name.GetIdentifierName());
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<LambdaExpression>(std::move(parameters), std::move(case_body)));
	arguments.push_back(std::move(case_operand));
	auto invoke_name = QualifiedName(Identifier(SYSTEM_CATALOG), Identifier(DEFAULT_SCHEMA), Identifier("invoke"));
	auto result = make_uniq<FunctionExpression>(invoke_name, std::move(arguments));
	result->SetAlias(expr.GetAlias());
	result->SetQueryLocation(expr.GetQueryLocation());
	return std::move(result);
}

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	if (expr.CaseOperand()) {
		if (expr.CaseChecks().size() == 1) {
			auto legacy_case = expr.GetLegacyCaseExpression();
			return BindExpression(*legacy_case, depth);
		}

		auto parameter_name = expr.CaseOperand()->GetName();
		auto operand_location = expr.CaseOperand()->GetQueryLocation();
		ErrorData error;
		BindChild(expr.CaseOperandMutable(), depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}

		auto &case_operand = BoundExpression::GetExpression(*expr.CaseOperandMutable());
		// Bound subqueries cannot be copied and must be passed to invoke.
		if (!case_operand->IsVolatile() && !case_operand->HasSubquery()) {
			auto legacy_case = CreateBoundLegacyCaseExpression(expr, *case_operand, operand_location);
			return BindExpression(*legacy_case, depth);
		}

		auto lambda_index = lambda_bindings ? lambda_bindings->size() : 0;
		auto invoke_expr = CreateCaseInvokeExpression(expr, lambda_index, std::move(parameter_name), operand_location,
		                                              std::move(expr.CaseOperandMutable()));
		// FIXME: Support subqueries and UNNEST without falling back to repeated operand evaluation.
		return BindExpression(invoke_expr, depth);
	}

	// first try to bind the children of the case expression
	ErrorData error;
	for (auto &check : expr.CaseChecksMutable()) {
		BindChild(check.when_expr, depth, error);
		BindChild(check.then_expr, depth, error);
	}
	BindChild(expr.ElseMutable(), depth, error);
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	// the children have been successfully resolved
	// figure out the result type of the CASE expression
	auto &else_expr = BoundExpression::GetExpression(*expr.ElseMutable());
	auto return_type = ExpressionBinder::GetExpressionReturnType(*else_expr);
	for (auto &check : expr.CaseChecksMutable()) {
		auto &then_expr = BoundExpression::GetExpression(*check.then_expr);
		auto then_type = ExpressionBinder::GetExpressionReturnType(*then_expr);
		if (!LogicalType::TryGetMaxLogicalType(context, return_type, then_type, return_type)) {
			throw BinderException(
			    expr, "Cannot mix values of type %s and %s in CASE expression - an explicit cast is required",
			    return_type.ToString(), then_type.ToString());
		}
	}

	// bind all the individual components of the CASE statement
	auto result = make_uniq<BoundCaseExpression>(return_type);
	for (auto &check : expr.CaseChecksMutable()) {
		auto &when_expr = BoundExpression::GetExpression(*check.when_expr);
		auto &then_expr = BoundExpression::GetExpression(*check.then_expr);
		BoundCaseCheck result_check;
		result_check.when_expr =
		    BoundCastExpression::AddCastToType(context, std::move(when_expr), LogicalType::BOOLEAN);
		result_check.then_expr = BoundCastExpression::AddCastToType(context, std::move(then_expr), return_type);
		result->CaseChecksMutable().push_back(std::move(result_check));
	}
	result->ElseMutable() = BoundCastExpression::AddCastToType(context, std::move(else_expr), return_type);
	return BindResult(std::move(result));
}
} // namespace duckdb
