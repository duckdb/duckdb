#include "duckdb/optimizer/rule/date_part_simplification.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/function/function.hpp"

using namespace duckdb;
using namespace std;

DatePartSimplificationRule::DatePartSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto func = make_unique<FunctionExpressionMatcher>();
	func->function = make_unique<SpecificFunctionMatcher>("date_part");
	func->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	func->matchers.push_back(make_unique<ExpressionMatcher>());
	func->policy = SetMatcher::Policy::ORDERED;
	root = move(func);
}

unique_ptr<Expression> DatePartSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                  bool &changes_made) {
	auto &date_part = (BoundFunctionExpression&) *bindings[0];
	auto &constant_expr = (BoundConstantExpression&) *bindings[1];
	auto &constant = constant_expr.value;

	if (constant.is_null) {
		// NULL specifier: return constant NULL
		return make_unique<BoundConstantExpression>(Value(date_part.return_type));
	}
	// otherwise check the specifier
	auto specifier = GetDatePartSpecifier(constant.str_value);
	string new_function_name;
	switch(specifier) {
	case DatePartSpecifier::YEAR:
		new_function_name = "year";
		break;
	default:
		return nullptr;
	}
	// found a replacement function: bind it
	vector<SQLType> arguments { date_part.function.arguments[1] };
	vector<unique_ptr<Expression>> children;
	children.push_back(move(date_part.children[1]));

	return ScalarFunction::BindScalarFunction(rewriter.context, DEFAULT_SCHEMA, new_function_name, arguments, move(children), false);
}
