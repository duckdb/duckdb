#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

static unique_ptr<Expression> CreateCaseComparison(ClientContext &context, const LogicalType &case_type,
                                                   const LogicalType &when_type, const LogicalType &input_type) {
	unique_ptr<Expression> case_ref = make_uniq<BoundReferenceExpression>(case_type, 0ULL);
	unique_ptr<Expression> when_ref = make_uniq<BoundReferenceExpression>(when_type, 1ULL);
	case_ref = BoundCastExpression::AddCastToType(context, std::move(case_ref), input_type,
	                                              input_type.id() == LogicalTypeId::ENUM);
	when_ref = BoundCastExpression::AddCastToType(context, std::move(when_ref), input_type,
	                                              input_type.id() == LogicalTypeId::ENUM);
	ExpressionBinder::PushCollation(context, case_ref, input_type);
	ExpressionBinder::PushCollation(context, when_ref, input_type);
	return BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(case_ref), std::move(when_ref));
}

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	ErrorData error;
	if (expr.CaseExpr()) {
		BindChild(expr.CaseExprMutable(), depth, error);
	}
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
	LogicalType case_sql_type;
	if (expr.CaseExpr()) {
		auto &case_expr = BoundExpression::GetExpression(*expr.CaseExprMutable());
		case_sql_type = ExpressionBinder::GetExpressionReturnType(*case_expr);
		result->CaseExprMutable() = std::move(case_expr);
	}
	for (auto &check : expr.CaseChecksMutable()) {
		auto &when_expr = BoundExpression::GetExpression(*check.when_expr);
		auto &then_expr = BoundExpression::GetExpression(*check.then_expr);
		BoundCaseCheck result_check;
		if (result->CaseExpr()) {
			auto when_sql_type = ExpressionBinder::GetExpressionReturnType(*when_expr);
			LogicalType input_type;
			if (!BoundComparisonExpression::TryBindComparison(context, case_sql_type, when_sql_type, input_type,
			                                                  ExpressionType::COMPARE_EQUAL)) {
				return BindResult(BinderException(
				    expr,
				    "Cannot compare values of type %s and type %s in CASE expression - an explicit cast is required",
				    case_sql_type.ToString(), when_sql_type.ToString()));
			}
			result_check.compare_expr = CreateCaseComparison(context, result->CaseExpr()->GetReturnType(),
			                                                 when_expr->GetReturnType(), input_type);
			result_check.when_expr = std::move(when_expr);
		} else {
			result_check.when_expr =
			    BoundCastExpression::AddCastToType(context, std::move(when_expr), LogicalType::BOOLEAN);
		}
		result_check.then_expr = BoundCastExpression::AddCastToType(context, std::move(then_expr), return_type);
		result->CaseChecksMutable().push_back(std::move(result_check));
	}
	result->ElseMutable() = BoundCastExpression::AddCastToType(context, std::move(else_expr), return_type);
	return BindResult(std::move(result));
}
} // namespace duckdb
