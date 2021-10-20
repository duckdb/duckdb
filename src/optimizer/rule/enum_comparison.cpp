#include "duckdb/optimizer/rule/enum_comparison.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/optimizer/matcher/type_matcher_id.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

EnumComparisonRule::EnumComparisonRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a CaseExpression that has a ConstantExpression as a check
	auto op = make_unique<ComparisonExpressionMatcher>();
	op->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);
	for (idx_t i = 0; i < 2; i++) {
		auto child = make_unique<CastExpressionMatcher>();
		child->type = make_unique<TypeMatcherId>(LogicalTypeId::VARCHAR);
		child->matcher = make_unique<ExpressionMatcher>();
		child->matcher->type = make_unique<TypeMatcherId>(LogicalTypeId::ENUM);
		op->matchers.push_back(move(child));
	}
	root = move(op);
}

bool AreMatchesPossible(LogicalType &left, LogicalType &right) {
	LogicalType *small, *big;
	if (EnumType::GetSize(left) < EnumType::GetSize(right)) {
		small = &left;
		big = &right;
	} else {
		small = &right;
		big = &left;
	}
	auto string_vec = EnumType::GetValuesInsertOrder(*small);
	for (auto &key : string_vec) {
		if (EnumType::GetPos(*big, key) != -1) {
			return true;
		}
	}
	return false;
}
unique_ptr<Expression> EnumComparisonRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                 bool &changes_made) {

	auto root = (BoundComparisonExpression *)bindings[0];
	auto left_child = (BoundCastExpression *)bindings[1];
	auto right_child = (BoundCastExpression *)bindings[3];

	if (!AreMatchesPossible(left_child->child->return_type, right_child->child->return_type)) {
		auto new_left = ExpressionRewriter::ConstantOrNull(move(root->left), Value::Numeric(LogicalType::UTINYINT, 0));
		auto new_right =
		    ExpressionRewriter::ConstantOrNull(move(root->right), Value::Numeric(LogicalType::UTINYINT, 1));
		return make_unique<BoundComparisonExpression>(root->type, move(new_left), move(new_right));
	}
	auto cast_left_to_right =
	    make_unique<BoundCastExpression>(move(left_child->child), right_child->child->return_type);

	return make_unique<BoundComparisonExpression>(root->type, move(cast_left_to_right), move(right_child->child));
}

} // namespace duckdb
