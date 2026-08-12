#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

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

static unique_ptr<CaseExpression> CreateLegacyCaseExpression(const CaseExpression &expr,
                                                             unique_ptr<Expression> case_operand) {
	auto result = make_uniq<CaseExpression>();
	const bool can_copy_bound_operand = !case_operand->HasParameter() && !case_operand->HasSubquery();
	for (idx_t i = 0; i < expr.CaseChecks().size(); i++) {
		auto &check = expr.CaseChecks()[i];
		unique_ptr<ParsedExpression> operand;
		if (can_copy_bound_operand) {
			auto bound_operand = i + 1 == expr.CaseChecks().size() ? std::move(case_operand) : case_operand->Copy();
			operand = make_uniq<BoundExpression>(std::move(bound_operand));
		} else if (i == 0) {
			operand = make_uniq<BoundExpression>(std::move(case_operand));
		} else {
			operand = expr.CaseOperand()->Copy();
		}

		CaseCheck legacy_check;
		legacy_check.when_expr =
		    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(operand), check.when_expr->Copy());
		legacy_check.then_expr = check.then_expr->Copy();
		result->CaseChecksMutable().push_back(std::move(legacy_check));
	}
	result->ElseMutable() = expr.Else().Copy();
	result->SetAlias(expr.GetAlias());
	result->SetQueryLocation(expr.GetQueryLocation());
	return result;
}

static unique_ptr<ParsedExpression> CreateCaseInvokeExpression(const CaseExpression &expr,
                                                               unique_ptr<Expression> case_operand) {
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
	arguments.push_back(make_uniq<BoundExpression>(std::move(case_operand)));
	auto invoke_name = QualifiedName(Identifier(SYSTEM_CATALOG), Identifier(DEFAULT_SCHEMA), Identifier("invoke"));
	auto result = make_uniq<FunctionExpression>(invoke_name, std::move(arguments));
	result->SetGeneratedSimpleCase();
	result->SetAlias(expr.GetAlias());
	result->SetQueryLocation(expr.GetQueryLocation());
	return std::move(result);
}

static BindResult UnsupportedVolatileCase(const CaseExpression &expr) {
	return BindResult(BinderException(
	    expr,
	    "Simple CASE expressions with a volatile operand and a subquery or UNNEST in a branch are not supported"));
}

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	if (expr.CaseOperand()) {
		auto case_operand = expr.CaseOperand()->Copy();
		ErrorData error;
		BindChild(case_operand, depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}
		auto bound_operand = std::move(BoundExpression::GetExpression(*case_operand));

		// Legacy lowering duplicates the operand when there is more than one check.
		if (expr.CaseChecks().size() == 1 || !bound_operand->IsVolatile()) {
			auto legacy_case = CreateLegacyCaseExpression(expr, std::move(bound_operand));
			return BindExpression(*legacy_case, depth);
		}

		auto invoke_expr = CreateCaseInvokeExpression(expr, std::move(bound_operand));
		try {
			auto result = BindExpression(invoke_expr, depth);
			if (result.HasError() && BinderException::IsUnsupportedLambdaExpression(result.error, true)) {
				return UnsupportedVolatileCase(expr);
			}
			return result;
		} catch (const BinderException &ex) {
			if (BinderException::IsUnsupportedLambdaExpression(ErrorData(ex), true)) {
				return UnsupportedVolatileCase(expr);
			}
			throw;
		}
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
