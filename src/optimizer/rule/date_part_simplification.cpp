#include "duckdb/optimizer/rule/date_part_simplification.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

DatePartSimplificationRule::DatePartSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto func = make_unique<FunctionExpressionMatcher>();
	func->function = make_unique<SpecificFunctionMatcher>("date_part");
	func->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	func->matchers.push_back(make_unique<ExpressionMatcher>());
	func->policy = SetMatcher::Policy::ORDERED;
	root = move(func);
}

unique_ptr<Expression> DatePartSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                         bool &changes_made, bool is_root) {
	auto &date_part = (BoundFunctionExpression &)*bindings[0];
	auto &constant_expr = (BoundConstantExpression &)*bindings[1];
	auto &constant = constant_expr.value;

	if (constant.IsNull()) {
		// NULL specifier: return constant NULL
		return make_unique<BoundConstantExpression>(Value(date_part.return_type));
	}
	// otherwise check the specifier
	auto specifier = GetDatePartSpecifier(StringValue::Get(constant));
	string new_function_name;
	switch (specifier) {
	case DatePartSpecifier::YEAR:
		new_function_name = "year";
		break;
	case DatePartSpecifier::MONTH:
		new_function_name = "month";
		break;
	case DatePartSpecifier::DAY:
		new_function_name = "day";
		break;
	case DatePartSpecifier::DECADE:
		new_function_name = "decade";
		break;
	case DatePartSpecifier::CENTURY:
		new_function_name = "century";
		break;
	case DatePartSpecifier::MILLENNIUM:
		new_function_name = "millennium";
		break;
	case DatePartSpecifier::QUARTER:
		new_function_name = "quarter";
		break;
	case DatePartSpecifier::WEEK:
		new_function_name = "week";
		break;
	case DatePartSpecifier::YEARWEEK:
		new_function_name = "yearweek";
		break;
	case DatePartSpecifier::DOW:
		new_function_name = "dayofweek";
		break;
	case DatePartSpecifier::ISODOW:
		new_function_name = "isodow";
		break;
	case DatePartSpecifier::DOY:
		new_function_name = "dayofyear";
		break;
	case DatePartSpecifier::EPOCH:
		new_function_name = "epoch";
		break;
	case DatePartSpecifier::MICROSECONDS:
		new_function_name = "microsecond";
		break;
	case DatePartSpecifier::MILLISECONDS:
		new_function_name = "millisecond";
		break;
	case DatePartSpecifier::SECOND:
		new_function_name = "second";
		break;
	case DatePartSpecifier::MINUTE:
		new_function_name = "minute";
		break;
	case DatePartSpecifier::HOUR:
		new_function_name = "hour";
		break;
	default:
		return nullptr;
	}
	// found a replacement function: bind it
	vector<unique_ptr<Expression>> children;
	children.push_back(move(date_part.children[1]));

	string error;
	auto function = ScalarFunction::BindScalarFunction(rewriter.context, DEFAULT_SCHEMA, new_function_name,
	                                                   move(children), error, false);
	if (!function) {
		throw BinderException(error);
	}
	return function;
}

} // namespace duckdb
