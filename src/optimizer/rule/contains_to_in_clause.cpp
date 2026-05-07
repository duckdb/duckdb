#include "duckdb/optimizer/rule/contains_to_in_clause.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

// ContainsToInClauseRule::ContainsToInClauseRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
// 	root = make_uniq<ExpressionMatcher>(ExpressionClass::BOUND_FUNCTION);
// }

ContainsToInClauseRule::ContainsToInClauseRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto func = make_uniq<FunctionExpressionMatcher>();
	func->function = make_uniq<SpecificFunctionMatcher>("contains");
	func->matchers.push_back(make_uniq<FoldableConstantMatcher>());
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->policy = SetMatcher::Policy::ORDERED;
	root = std::move(func);
}

unique_ptr<Expression> ContainsToInClauseRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                     bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &list_arg = expr.children[0];
	auto &probe_arg = expr.children[1];

	if (probe_arg->IsFoldable()) {
		return nullptr;
	}

	Value list_val;
	if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), *list_arg, list_val)) {
		return nullptr;
	}
	if (list_val.IsNull() || list_val.type().id() != LogicalTypeId::LIST) {
		return nullptr;
	}

	auto in_expr = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	in_expr->children.push_back(probe_arg->Copy());
	const auto &child_type = ListType::GetChildType(list_val.type());
	for (const auto &elem : ListValue::GetChildren(list_val)) {
		Value v = elem.DefaultCastAs(child_type);
		in_expr->children.push_back(make_uniq<BoundConstantExpression>(std::move(v)));
	}
	changes_made = true;
	return std::move(in_expr);
}

} // namespace duckdb
