#include "duckdb/optimizer/rule/date_trunc_simplification.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

DateTruncSimplificationRule::DateTruncSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto op = make_uniq<ComparisonExpressionMatcher>();

	auto lhs = make_uniq<FunctionExpressionMatcher>();
	lhs->function = make_uniq<ManyFunctionMatcher>(unordered_set<string> {"date_trunc", "datetrunc"});
	lhs->matchers.push_back(make_uniq<ConstantExpressionMatcher>());
	lhs->matchers.push_back(make_uniq<ExpressionMatcher>());
	lhs->policy = SetMatcher::Policy::ORDERED;

	auto rhs = make_uniq<ConstantExpressionMatcher>();

	op->matchers.push_back(std::move(lhs));
	op->matchers.push_back(std::move(rhs));
	op->policy = SetMatcher::Policy::UNORDERED;

	root = std::move(op);
}

unique_ptr<Expression> DateTruncSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                          bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundComparisonExpression>();
	auto comparison_type = expr.GetExpressionType();

	auto &date_part = bindings[2].get().Cast<BoundConstantExpression>();
	// We must have only a column on the LHS.
	if (bindings[3].get().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}

	auto &column_part = bindings[3].get().Cast<BoundColumnRefExpression>();
	auto &rhs = bindings[4].get().Cast<BoundConstantExpression>();

	// Determine whether or not the column name is on the lhs or rhs.
	const bool col_is_lhs = (expr.left->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION);

	// We want to treat rhs >= col equivalently to col <= rhs.
	// So, get the expression type if it was ordered such that the constant was actually on the right hand side.
	ExpressionType rhs_comparison_type = comparison_type;
	if (!col_is_lhs) {
		rhs_comparison_type = FlipComparisonExpression(comparison_type);
	}

	// Check whether trunc(date_part, constant_rhs) = constant_rhs.
	const bool is_truncated = DateIsTruncated(date_part, rhs);

	switch (rhs_comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		// We handle two very similar optimizations here:
		//
		// date_trunc(part, column) = constant_rhs  -->  column >= date_trunc(part, constant_rhs) AND
		//                                               column < date_trunc(part, date_add(constant_rhs,
		//                                                                                  INTERVAL 1 part))
		//    or, if date_trunc(part, constant_rhs) <> constant_rhs, this is unsatisfiable
		//
		// ----
		//
		// date_trunc(part, column) IS NOT DISTINCT FROM constant_rhs
		//
		//   Here we have two cases: when constant_rhs is NULL, this simplifies to:
		//
		// column IS NULL
		//
		//   Otherwise, the expression becomes:
		//
		// (column >= date_trunc(part, constant_rhs) AND
		//  column < date_trunc(part, date_add(constant_rhs, INTERVAL 1 part)) AND
		//  column IS NOT NULL)
		//
		{
			// First check if we can just return `column IS NULL`.
			if (rhs_comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM && rhs.value.IsNull()) {
				auto op = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
				op->children.push_back(column_part.Copy());
				return std::move(op);
			} else {
				if (!is_truncated) {
					return make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
				}

				auto trunc = CreateTrunc(date_part, rhs, column_part.return_type);
				if (!trunc) {
					return nullptr;
				}

				auto trunc_add = CreateTruncAdd(date_part, rhs, column_part.return_type);
				if (!trunc_add) {
					return nullptr;
				}

				auto gteq = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
				                                                 column_part.Copy(), std::move(trunc));
				auto lt = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, column_part.Copy(),
				                                               std::move(trunc_add));

				// For IS NOT DISTINCT FROM, we also have to add the extra NULL term.
				if (rhs_comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
					auto comp = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(gteq),
					                                                  std::move(lt));

					auto isnotnull =
					    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
					isnotnull->children.push_back(column_part.Copy());

					return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(comp),
					                                             std::move(isnotnull));
				} else {
					return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(gteq),
					                                             std::move(lt));
				}
			}
		}

	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		// We handle two very similar optimizations here:
		//
		// date_trunc(part, column) <> constant_rhs  -->  column < date_trunc(part, constant_rhs) OR
		//                                                column >= date_trunc(part, date_add(constant_rhs,
		//                                                                                    INTERVAL 1 part))
		//   or, if date_trunc(part, constant_rhs) <> constant_rhs, this is always true
		//
		// ----
		//
		// date_trunc(part, column) IS DISTINCT FROM constant_rhs
		//
		//   Here we have two cases: when constant_rhs is NULL, this simplifies to:
		//
		// column IS NOT NULL
		//
		//   Otherwise, the expression becomes:
		//
		// (column < date_trunc(part, constant_rhs) OR
		//  column >= date_trunc(part, date_add(constant_rhs, INTERVAL 1 part)) OR
		//  column IS NULL)
		//
		{
			if (rhs_comparison_type == ExpressionType::COMPARE_DISTINCT_FROM && rhs.value.IsNull()) {
				// Return 'column IS NOT NULL'.
				auto op =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				op->children.push_back(column_part.Copy());
				return std::move(op);
			} else {
				if (!is_truncated) {
					return make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
				}

				auto trunc = CreateTrunc(date_part, rhs, column_part.return_type);
				if (!trunc) {
					return nullptr;
				}

				auto trunc_add = CreateTruncAdd(date_part, rhs, column_part.return_type);
				if (!trunc_add) {
					return nullptr;
				}

				auto lt = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, column_part.Copy(),
				                                               std::move(trunc));
				auto gteq = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
				                                                 column_part.Copy(), std::move(trunc_add));

				// If this is a DISTINCT FROM, we need to add the 'column IS NULL' term.
				if (rhs_comparison_type == ExpressionType::COMPARE_DISTINCT_FROM) {
					auto comp = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(gteq),
					                                                  std::move(lt));

					auto isnull =
					    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
					isnull->children.push_back(column_part.Copy());

					return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(comp),
					                                             std::move(isnull));
				} else {
					return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(gteq),
					                                             std::move(lt));
				}
			}
		}
		return nullptr;

	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// date_trunc(part, column) <  constant_rhs  -->  column <  date_trunc(part, date_add(constant_rhs,
		//                                                                                    INTERVAL 1 part))
		// date_trunc(part, column) >= constant_rhs  -->  column >= date_trunc(part, date_add(constant_rhs,
		//                                                                                    INTERVAL 1 part))
		{
			// The optimization for < and >= is a little tricky: if trunc(rhs) = rhs, then we need to just
			// use the rhs as-is, instead of using trunc(rhs + 1 date_part).
			if (!is_truncated) {
				// Create date_trunc(part, date_add(rhs, INTERVAL 1 part)) and fold the constant.
				auto trunc = CreateTruncAdd(date_part, rhs, column_part.return_type);
				if (!trunc) {
					return nullptr; // Something went wrong---don't do the optimization.
				}

				if (col_is_lhs) {
					expr.left = column_part.Copy();
					expr.right = std::move(trunc);
				} else {
					expr.right = column_part.Copy();
					expr.left = std::move(trunc);
				}
			} else {
				// If the RHS is already truncated (i.e.  date_trunc(part, rhs) = rhs), then we can use
				// it as-is.
				if (col_is_lhs) {
					expr.left = column_part.Copy();
					// Determine whether the RHS needs to be casted.
					if (rhs.return_type.id() != expr.left->return_type.id()) {
						expr.right = CastAndEvaluate(std::move(expr.right), expr.left->return_type);
					}
				} else {
					expr.right = column_part.Copy();
					// Determine whether the RHS needs to be casted.
					if (rhs.return_type.id() != expr.right->return_type.id()) {
						expr.left = CastAndEvaluate(std::move(expr.left), expr.right->return_type);
					}
				}
			}

			changes_made = true;
			return nullptr;
		}

	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
		// date_trunc(part, column) <= constant_rhs  -->  column <  date_trunc(part, date_add(constant_rhs,
		//                                                                                    INTERVAL 1 part))
		// date_trunc(part, column) >  constant_rhs  -->  column >= date_trunc(part, date_add(constant_rhs,
		//                                                                                    INTERVAL 1 part))
		{
			// Create date_trunc(part, date_add(rhs, INTERVAL 1 part)) and fold the constant.
			auto trunc = CreateTruncAdd(date_part, rhs, column_part.return_type);
			if (!trunc) {
				return nullptr; // Something went wrong---don't do the optimization.
			}

			if (col_is_lhs) {
				expr.left = column_part.Copy();
				expr.right = std::move(trunc);
			} else {
				expr.right = column_part.Copy();
				expr.left = std::move(trunc);
			}

			// > needs to become >=, and <= needs to become <.
			if (rhs_comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
				if (col_is_lhs) {
					expr.SetExpressionTypeUnsafe(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
				} else {
					expr.SetExpressionTypeUnsafe(ExpressionType::COMPARE_LESSTHANOREQUALTO);
				}
			} else {
				if (col_is_lhs) {
					expr.SetExpressionTypeUnsafe(ExpressionType::COMPARE_LESSTHAN);
				} else {
					expr.SetExpressionTypeUnsafe(ExpressionType::COMPARE_GREATERTHAN);
				}
			}

			changes_made = true;
			return nullptr;
		}

	default:
		return nullptr;
	}
}

string DateTruncSimplificationRule::DatePartToFunc(const DatePartSpecifier &date_part) {
	switch (date_part) {
	// These specifiers can be used as intervals.
	case DatePartSpecifier::YEAR:
		return "to_years";
	case DatePartSpecifier::MONTH:
		return "to_months";
	case DatePartSpecifier::DAY:
		return "to_days";
	case DatePartSpecifier::DECADE:
		return "to_decades";
	case DatePartSpecifier::CENTURY:
		return "to_centuries";
	case DatePartSpecifier::MILLENNIUM:
		return "to_millennia";
	case DatePartSpecifier::MICROSECONDS:
		return "to_microseconds";
	case DatePartSpecifier::MILLISECONDS:
		return "to_milliseconds";
	case DatePartSpecifier::SECOND:
		return "to_seconds";
	case DatePartSpecifier::MINUTE:
		return "to_minutes";
	case DatePartSpecifier::HOUR:
		return "to_hours";
	case DatePartSpecifier::WEEK:
		return "to_weeks";
	case DatePartSpecifier::QUARTER:
		return "to_quarters";

	// These specifiers cannot be used as intervals and can only be used as
	// date parts.
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::ISOYEAR:
	case DatePartSpecifier::YEARWEEK:
	case DatePartSpecifier::ERA:
	case DatePartSpecifier::TIMEZONE:
	case DatePartSpecifier::TIMEZONE_HOUR:
	case DatePartSpecifier::TIMEZONE_MINUTE:
	default:
		return "";
	}
}

unique_ptr<Expression> DateTruncSimplificationRule::CreateTrunc(const BoundConstantExpression &date_part,
                                                                const BoundConstantExpression &rhs,
                                                                const LogicalType &return_type) {
	FunctionBinder binder(rewriter.context);
	ErrorData error;

	vector<unique_ptr<Expression>> args;
	args.emplace_back(date_part.Copy());
	args.emplace_back(rhs.Copy());
	auto trunc = binder.BindScalarFunction(DEFAULT_SCHEMA, "date_trunc", std::move(args), error);

	// Ensure that the RHS type matches the column type.
	if (trunc->return_type.id() != return_type.id()) {
		trunc = BoundCastExpression::AddDefaultCastToType(std::move(trunc), return_type, true);
	}

	if (trunc->IsFoldable()) {
		Value result;
		if (!ExpressionExecutor::TryEvaluateScalar(rewriter.context, *trunc, result)) {
			return trunc;
		}

		return make_uniq<BoundConstantExpression>(result);
	}

	return trunc;
}

unique_ptr<Expression> DateTruncSimplificationRule::CreateTruncAdd(const BoundConstantExpression &date_part,
                                                                   const BoundConstantExpression &rhs,
                                                                   const LogicalType &return_type) {
	DatePartSpecifier part = GetDatePartSpecifier(StringValue::Get(date_part.value));
	const string interval_func_name = DatePartToFunc(part);

	// If the date part cannot be represented as an interval, then we cannot
	// perform the optimization.
	if (interval_func_name.empty()) {
		return nullptr;
	}

	FunctionBinder binder(rewriter.context);
	ErrorData error;

	vector<unique_ptr<Expression>> args1;
	auto constant_param = make_uniq<BoundConstantExpression>(Value::INTEGER(1));
	args1.emplace_back(std::move(constant_param));
	auto interval = binder.BindScalarFunction(DEFAULT_SCHEMA, interval_func_name, std::move(args1), error);
	if (!interval) {
		return nullptr; // Something wrong---just don't do the optimization.
	}

	vector<unique_ptr<Expression>> args2;
	args2.emplace_back(rhs.Copy());
	args2.emplace_back(std::move(interval));
	auto add = binder.BindScalarFunction(DEFAULT_SCHEMA, "+", std::move(args2), error);

	vector<unique_ptr<Expression>> args3;
	args3.emplace_back(date_part.Copy());
	args3.emplace_back(std::move(add));
	auto trunc = binder.BindScalarFunction(DEFAULT_SCHEMA, "date_trunc", std::move(args3), error);

	// Ensure that the RHS type matches the column type.
	if (trunc->return_type.id() != return_type.id()) {
		trunc = BoundCastExpression::AddDefaultCastToType(std::move(trunc), return_type, true);
	}

	if (trunc->IsFoldable()) {
		Value result;
		if (!ExpressionExecutor::TryEvaluateScalar(rewriter.context, *trunc, result)) {
			return trunc;
		}

		return make_uniq<BoundConstantExpression>(result);
	}

	return trunc;
}

bool DateTruncSimplificationRule::DateIsTruncated(const BoundConstantExpression &date_part,
                                                  const BoundConstantExpression &rhs) {
	// If the rhs is null, then the date is "truncated" in the sense that date_trunc(..., NULL) is also NULL.
	if (rhs.value.IsNull()) {
		return true;
	}

	// Create the node date_trunc(date_part, rhs).
	auto trunc = CreateTrunc(date_part, rhs, rhs.return_type);

	Value trunc_result, result;
	if (!ExpressionExecutor::TryEvaluateScalar(rewriter.context, *trunc, trunc_result)) {
		return false;
	}
	if (!ExpressionExecutor::TryEvaluateScalar(rewriter.context, rhs, result)) {
		return false;
	}

	return (result == trunc_result);
}

unique_ptr<Expression> DateTruncSimplificationRule::CastAndEvaluate(unique_ptr<Expression> rhs,
                                                                    const LogicalType &return_type) {
	auto cast = BoundCastExpression::AddDefaultCastToType(std::move(rhs), return_type, true);
	if (cast->IsFoldable()) {
		Value result;
		if (!ExpressionExecutor::TryEvaluateScalar(rewriter.context, *cast, result)) {
			return cast;
		}

		return make_uniq<BoundConstantExpression>(result);
	}

	return cast;
}

} // namespace duckdb
