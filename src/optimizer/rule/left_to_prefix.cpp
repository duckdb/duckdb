#include "duckdb/optimizer/rule/left_to_prefix.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/matcher/type_matcher_id.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

LeftToPrefixRule::LeftToPrefixRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match [left(s, n) = 'constant'] (in any operand order)
	auto op = make_uniq<ComparisonExpressionMatcher>();
	op->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);
	op->policy = SetMatcher::Policy::UNORDERED;

	auto func = make_uniq<FunctionExpressionMatcher>();
	func->function = make_uniq<SpecificFunctionMatcher>("left");
	func->type = make_uniq<TypeMatcherId>(LogicalTypeId::VARCHAR);
	auto count_matcher = make_uniq<ConstantExpressionMatcher>();
	count_matcher->type = make_uniq<IntegerTypeMatcher>();
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->matchers.push_back(std::move(count_matcher));
	func->policy = SetMatcher::Policy::ORDERED;
	op->matchers.push_back(std::move(func));

	auto constant = make_uniq<ConstantExpressionMatcher>();
	constant->type = make_uniq<TypeMatcherId>(LogicalTypeId::VARCHAR);
	op->matchers.push_back(std::move(constant));

	root = std::move(op);
}

unique_ptr<Expression> LeftToPrefixRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                               bool &changes_made, bool is_root) {
	auto &comparison = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &func = bindings[1].get().Cast<BoundFunctionExpression>();
	auto &count_constant = bindings[3].get().Cast<BoundConstantExpression>();
	auto &constant = bindings[4].get().Cast<BoundConstantExpression>();

	// [left(s, n) = NULL] and [left(s, NULL) = c] are always NULL
	if (constant.GetValue().IsNull() || count_constant.GetValue().IsNull()) {
		return make_uniq<BoundConstantExpression>(Value(comparison.GetReturnType()));
	}

	// Left counts from the end of the string for negative values: don't rewrite
	auto num_characters = count_constant.GetValue().GetValue<int64_t>();
	if (num_characters < 0 || num_characters > NumericLimits<uint32_t>::Maximum()) {
		return nullptr;
	}

	auto &children = func.GetChildrenMutable();
	auto constant_str = StringValue::Get(constant.GetValue());
	auto constant_length = Length<string_t, int64_t>(string_t(constant_str.c_str(), constant_str.size()));

	// The constant is longer than the extracted prefix, so the comparison can only be FALSE
	if (constant_length > num_characters) {
		return ExpressionRewriter::ConstantOrNull(std::move(children[0]), Value::BOOLEAN(false));
	}

	// left can only return fewer than n characters if it returns the entire string, so this is an equality comparison
	// on the string itself
	if (constant_length < num_characters) {
		return BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, std::move(children[0]),
		                                         make_uniq<BoundConstantExpression>(constant.GetValue()));
	}

	// Convert to prefix comparison
	vector<unique_ptr<Expression>> prefix_children;
	prefix_children.emplace_back(std::move(children[0]));
	prefix_children.emplace_back(make_uniq<BoundConstantExpression>(constant.GetValue()));
	auto prefix_expr = PrefixFun::GetFunction().Bind(GetContext(), std::move(prefix_children));
	return std::move(prefix_expr);
}

} // namespace duckdb
