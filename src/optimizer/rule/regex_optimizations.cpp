#include "duckdb/optimizer/rule/regex_optimizations.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "re2/re2.h"
#include "re2/regexp.h"

namespace duckdb {

RegexOptimizationRule::RegexOptimizationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto func = make_unique<FunctionExpressionMatcher>();
	func->function = make_unique<SpecificFunctionMatcher>("regexp_matches");
	func->policy = SetMatcher::Policy::ORDERED;
	func->matchers.push_back(make_unique<ExpressionMatcher>());
	func->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	root = move(func);
}

unique_ptr<Expression> RegexOptimizationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                    bool &changes_made, bool is_root) {
	auto root = (BoundFunctionExpression *)bindings[0];
	auto constant_expr = (BoundConstantExpression *)bindings[2];
	D_ASSERT(root->children.size() == 2);

	if (constant_expr->value.IsNull()) {
		return make_unique<BoundConstantExpression>(Value(root->return_type));
	}

	// the constant_expr is a scalar expression that we have to fold
	if (!constant_expr->IsFoldable()) {
		return nullptr;
	}

	auto constant_value = ExpressionExecutor::EvaluateScalar(*constant_expr);
	D_ASSERT(constant_value.type() == constant_expr->return_type);
	auto &patt_str = StringValue::Get(constant_value);

	duckdb_re2::RE2 pattern(patt_str);
	if (!pattern.ok()) {
		return nullptr; // this should fail somewhere else
	}

	if (pattern.Regexp()->op() == duckdb_re2::kRegexpLiteralString ||
	    pattern.Regexp()->op() == duckdb_re2::kRegexpLiteral) {
		auto contains = make_unique<BoundFunctionExpression>(root->return_type, ContainsFun::GetFunction(),
		                                                     move(root->children), nullptr);

		contains->children[1] = make_unique<BoundConstantExpression>(Value(patt_str));
		return move(contains);
	}
	return nullptr;
}

} // namespace duckdb
