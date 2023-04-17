#include "duckdb/optimizer/rule/empty_needle_removal.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"

namespace duckdb {

EmptyNeedleRemovalRule::EmptyNeedleRemovalRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a FunctionExpression that has a foldable ConstantExpression
	auto func = make_uniq<FunctionExpressionMatcher>();
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->policy = SetMatcher::Policy::SOME;

	unordered_set<string> functions = {"prefix", "contains", "suffix"};
	func->function = make_uniq<ManyFunctionMatcher>(functions);
	root = std::move(func);
}

unique_ptr<Expression> EmptyNeedleRemovalRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                     bool &changes_made, bool is_root) {
	auto &root = bindings[0].get().Cast<BoundFunctionExpression>();
	D_ASSERT(root.children.size() == 2);
	auto &prefix_expr = bindings[2].get();

	// the constant_expr is a scalar expression that we have to fold
	if (!prefix_expr.IsFoldable()) {
		return nullptr;
	}
	D_ASSERT(root.return_type.id() == LogicalTypeId::BOOLEAN);

	auto prefix_value = ExpressionExecutor::EvaluateScalar(GetContext(), prefix_expr);

	if (prefix_value.IsNull()) {
		return make_uniq<BoundConstantExpression>(Value(LogicalType::BOOLEAN));
	}

	D_ASSERT(prefix_value.type() == prefix_expr.return_type);
	auto &needle_string = StringValue::Get(prefix_value);

	// PREFIX('xyz', '') is TRUE
	// PREFIX(NULL, '') is NULL
	// so rewrite PREFIX(x, '') to TRUE_OR_NULL(x)
	if (needle_string.empty()) {
		return ExpressionRewriter::ConstantOrNull(std::move(root.children[0]), Value::BOOLEAN(true));
	}
	return nullptr;
}

} // namespace duckdb
