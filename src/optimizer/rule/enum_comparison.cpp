#include "duckdb/optimizer/rule/enum_comparison.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/optimizer/matcher/type_matcher_id.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

EnumComparisonRule::EnumComparisonRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that is an Equality and has a VARCHAR and ENUM as its children
	auto op = make_unique<ComparisonExpressionMatcher>();
	// Enum requires expression to be root
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
	LogicalType *small_enum, *big_enum;
	if (EnumType::GetSize(left) < EnumType::GetSize(right)) {
		small_enum = &left;
		big_enum = &right;
	} else {
		small_enum = &right;
		big_enum = &left;
	}
	auto &string_vec = EnumType::GetValuesInsertOrder(*small_enum);
	auto string_vec_ptr = FlatVector::GetData<string_t>(string_vec);
	auto size = EnumType::GetSize(*small_enum);
	for (idx_t i = 0; i < size; i++) {
		auto key = string_vec_ptr[i].GetString();
		if (EnumType::GetPos(*big_enum, key) != -1) {
			return true;
		}
	}
	return false;
}
unique_ptr<Expression> EnumComparisonRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                 bool &changes_made, bool is_root) {

	auto root = (BoundComparisonExpression *)bindings[0];
	auto left_child = (BoundCastExpression *)bindings[1];
	auto right_child = (BoundCastExpression *)bindings[3];

	if (!AreMatchesPossible(left_child->child->return_type, right_child->child->return_type)) {
		vector<unique_ptr<Expression>> children;
		children.push_back(move(root->left));
		children.push_back(move(root->right));
		return ExpressionRewriter::ConstantOrNull(move(children), Value::BOOLEAN(false));
	}

	if (!is_root || op.type != LogicalOperatorType::LOGICAL_FILTER) {
		return nullptr;
	}

	auto cast_left_to_right =
	    make_unique<BoundCastExpression>(move(left_child->child), right_child->child->return_type, true);

	return make_unique<BoundComparisonExpression>(root->type, move(cast_left_to_right), move(right_child->child));
}

} // namespace duckdb
