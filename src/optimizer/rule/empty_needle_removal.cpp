#include "duckdb/optimizer/rule/empty_needle_removal.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

#include <regex>

using namespace duckdb;
using namespace std;

EmptyNeedleRemovalRule::EmptyNeedleRemovalRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a FunctionExpression that has a foldable ConstantExpression
	auto func = make_unique<FunctionExpressionMatcher>();
	func->matchers.push_back(make_unique<ExpressionMatcher>());
	func->matchers.push_back(make_unique<ExpressionMatcher>());
	func->policy = SetMatcher::Policy::SOME;

	unordered_set<string> functions = {"prefix", "contains", "suffix"};
	func->function = make_unique<ManyFunctionMatcher>(functions);
	root = move(func);
}

unique_ptr<Expression> EmptyNeedleRemovalRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                     bool &changes_made) {
	auto root = (BoundFunctionExpression *)bindings[0];
	assert(root->children.size() == 2);
	auto prefix_expr = bindings[2];

	// the constant_expr is a scalar expression that we have to fold
	if (!prefix_expr->IsFoldable()) {
		return nullptr;
	}
	assert(root->return_type == TypeId::BOOL);

	auto prefix_value = ExpressionExecutor::EvaluateScalar(*prefix_expr);

	if (prefix_value.is_null) {
		return make_unique<BoundConstantExpression>(Value(TypeId::BOOL));
	}

	assert(prefix_value.type == prefix_expr->return_type);
	string needle_string = string(((string_t)prefix_value.str_value).GetData());

	/* PREFIX('xyz', '') is TRUE, PREFIX(NULL, '') is NULL, so rewrite PREFIX(x, '') to (CASE WHEN x IS NOT NULL THEN
	 * TRUE ELSE NULL END) */
	if (needle_string.empty()) {
		auto if_ = make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, TypeId::BOOL);
		if_->children.push_back(bindings[1]->Copy());
		auto case_ =
		    make_unique<BoundCaseExpression>(move(if_), make_unique<BoundConstantExpression>(Value::BOOLEAN(true)),
		                                     make_unique<BoundConstantExpression>(Value(TypeId::BOOL)));
		return move(case_);
	}

	return nullptr;
}
