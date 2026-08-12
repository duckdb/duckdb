#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

static bool ContainsUnnest(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &function = expr.Cast<FunctionExpression>();
		if (ExpressionBinder::IsUnnestFunction(function.FunctionName())) {
			return true;
		}
	}
	for (auto &child : expr.Children()) {
		if (ContainsUnnest(child)) {
			return true;
		}
	}
	return false;
}

static bool PreventsInvoke(const ParsedExpression &expr) {
	return expr.HasSubquery() || ContainsUnnest(expr);
}

static bool CaseBodyPreventsInvoke(const CaseExpression &expr) {
	for (auto &check : expr.CaseChecks()) {
		if (PreventsInvoke(*check.when_expr) || PreventsInvoke(*check.then_expr)) {
			return true;
		}
	}
	return PreventsInvoke(expr.Else());
}

static void CollectColumnNames(const ParsedExpression &expr, unordered_set<string> &column_names) {
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(expr, [&](const ColumnRefExpression &column_ref) {
		column_names.insert(StringUtil::Lower(column_ref.GetColumnName().GetIdentifierName()));
	});
}

static string GetCaseParameterName(const CaseExpression &expr) {
	unordered_set<string> column_names;
	for (auto &check : expr.CaseChecks()) {
		CollectColumnNames(*check.when_expr, column_names);
		CollectColumnNames(*check.then_expr, column_names);
	}
	CollectColumnNames(expr.Else(), column_names);

	constexpr const char *base_name = "__duckdb_simple_case_subject";
	string result = base_name;
	for (idx_t suffix = 0; column_names.find(StringUtil::Lower(result)) != column_names.end(); suffix++) {
		result = StringUtil::Format("%s_%llu", base_name, suffix);
	}
	return result;
}

static unique_ptr<ParsedExpression> CreateCaseInvokeExpression(const CaseExpression &expr) {
	auto parameter_name = GetCaseParameterName(expr);
	auto case_body = make_uniq<CaseExpression>();
	for (auto &check : expr.CaseChecks()) {
		CaseCheck invoke_check;
		invoke_check.when_expr = make_uniq<ComparisonExpression>(
		    ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>(Identifier(parameter_name)),
		    check.when_expr->Copy());
		invoke_check.then_expr = check.then_expr->Copy();
		case_body->CaseChecksMutable().push_back(std::move(invoke_check));
	}
	case_body->ElseMutable() = expr.Else().Copy();

	vector<string> parameters;
	parameters.push_back(parameter_name);
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<LambdaExpression>(std::move(parameters), std::move(case_body)));
	arguments.push_back(expr.CaseOperand()->Copy());
	auto result = make_uniq<FunctionExpression>("invoke", std::move(arguments));
	result->SetAlias(expr.GetAlias());
	result->SetQueryLocation(expr.GetQueryLocation());
	return std::move(result);
}

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	if (expr.CaseOperand()) {
		if (!CaseBodyPreventsInvoke(expr)) {
			auto invoke_expr = CreateCaseInvokeExpression(expr);
			return BindExpression(invoke_expr, depth);
		}

		auto legacy_case = expr.GetLegacyCaseExpression();
		auto legacy_result = BindExpression(*legacy_case, depth);
		if (legacy_result.HasError()) {
			return legacy_result;
		}
		auto &bound_case = legacy_result.expression->Cast<BoundCaseExpression>();
		D_ASSERT(!bound_case.CaseChecks().empty());
		auto &first_check = *bound_case.CaseChecks()[0].when_expr;
		D_ASSERT(BoundComparisonExpression::IsComparison(first_check));
		auto &comparison = first_check.Cast<BoundFunctionExpression>();
		if (BoundComparisonExpression::Left(comparison).IsVolatile()) {
			return BindResult(BinderException(
			    expr, "Simple CASE expressions with a volatile operand and a subquery or UNNEST in a branch are not "
			          "supported"));
		}
		return legacy_result;
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
