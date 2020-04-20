#include "duckdb/optimizer/rule/empty_prefix_removal.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <regex>

using namespace duckdb;
using namespace std;

EmptyPrefixRemovalRule::EmptyPrefixRemovalRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a FunctionExpression that has a foldable ConstantExpression
	auto func = make_unique<FunctionExpressionMatcher>();
	func->matchers.push_back(make_unique<ExpressionMatcher>());
	func->matchers.push_back(make_unique<ExpressionMatcher>());
	func->policy = SetMatcher::Policy::SOME;
	func->function = make_unique<SpecificFunctionMatcher>("prefix");
	root = move(func);
}

unique_ptr<Expression> EmptyPrefixRemovalRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                     bool &changes_made) {
	auto root = (BoundFunctionExpression *)bindings[0];
	assert(root->children.size() == 2);
	auto prefix_expr = bindings[2];

	// the constant_expr is a scalar expression that we have to fold
	if (!prefix_expr->IsFoldable()) {
		return nullptr;
	}

	auto prefix_value = ExpressionExecutor::EvaluateScalar(*prefix_expr);

	if (prefix_value.is_null) {
		return make_unique<BoundConstantExpression>(Value(root->return_type));
	}

	assert(prefix_value.type == prefix_expr->return_type);
	string needle_string = string(((string_t)prefix_value.str_value).GetData());

	if (needle_string.empty()) {
		return bindings[1]->Copy();
	}

	return nullptr;
}
