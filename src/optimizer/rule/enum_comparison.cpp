#include "duckdb/optimizer/rule/enum_comparison.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

EnumComparisonRule::EnumComparisonRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a CaseExpression that has a ConstantExpression as a check
	auto op = make_unique<ComparisonExpressionMatcher>();
	op->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);
	for (idx_t i = 0; i < 2; i++) {
		auto child = make_unique<CastExpressionMatcher>();
		child->type = make_unique<SpecificTypeMatcher>(LogicalTypeId::VARCHAR);
		child->matcher = make_unique<ExpressionMatcher>();
		child->matcher->type = make_unique<SpecificTypeMatcher>(LogicalTypeId::ENUM);
		op->matchers.push_back(move(child));
	}
	root = move(op);
}

unique_ptr<Expression> EnumComparisonRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                 bool &changes_made) {

	auto root = (BoundComparisonExpression *)bindings[0];
	auto left_child = (BoundCastExpression *)bindings[1];
	auto right_child = (BoundCastExpression *)bindings[3];

	return make_unique<BoundComparisonExpression>(root->type, left_child->child->Copy(), right_child->child->Copy());
}

} // namespace duckdb
