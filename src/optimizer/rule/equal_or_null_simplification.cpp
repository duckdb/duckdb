#include "duckdb/optimizer/rule/equal_or_null_simplification.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

EqualOrNullSimplification::EqualOrNullSimplification(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on OR conjunction
	auto op = make_unique<ConjunctionExpressionMatcher>();
	op->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::CONJUNCTION_OR);
	op->policy = SetMatcher::Policy::SOME;

	// equi comparison on one side
	auto equal_child = make_unique<ComparisonExpressionMatcher>();
	equal_child->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);
	equal_child->policy = SetMatcher::Policy::SOME;
	op->matchers.push_back(move(equal_child));

	// AND conjuction on the other
	auto and_child = make_unique<ConjunctionExpressionMatcher>();
	and_child->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::CONJUNCTION_AND);
	and_child->policy = SetMatcher::Policy::SOME;

	// IS NULL tests inside AND
	auto isnull_child = make_unique<ExpressionMatcher>();
	isnull_child->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::OPERATOR_IS_NULL);
	// I could try to use std::make_unique for a copy, but it's available from C++14 only
	auto isnull_child2 = make_unique<ExpressionMatcher>();
	isnull_child2->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::OPERATOR_IS_NULL);
	and_child->matchers.push_back(move(isnull_child));
	and_child->matchers.push_back(move(isnull_child2));

	op->matchers.push_back(move(and_child));
	root = move(op);
}

// a=b OR (a IS NULL AND b IS NULL) to a IS NOT DISTINCT FROM b
static unique_ptr<Expression> TryRewriteEqualOrIsNull(const Expression *equal_expr, const Expression *and_expr) {
	if (equal_expr->type != ExpressionType::COMPARE_EQUAL || and_expr->type != ExpressionType::CONJUNCTION_AND) {
		return nullptr;
	}

	const auto equal_cast = (BoundComparisonExpression *)equal_expr;
	const auto and_cast = (BoundConjunctionExpression *)and_expr;

	if (and_cast->children.size() != 2) {
		return nullptr;
	}

	// Make sure on the AND conjuction the relevant conditions appear
	const auto a_exp = equal_cast->left.get();
	const auto b_exp = equal_cast->right.get();
	bool valid = true;
	bool a_is_null_found = false;
	bool b_is_null_found = false;

	for (const auto &item : and_cast->children) {
		const auto next_exp = item.get();

		if (next_exp->type == ExpressionType::OPERATOR_IS_NULL) {
			const auto next_exp_cast = (BoundOperatorExpression *)next_exp;
			const auto child = next_exp_cast->children[0].get();

			// Test for equality on both 'a' and 'b' expressions
			if (Expression::Equals(child, a_exp)) {
				a_is_null_found = true;
			} else if (Expression::Equals(child, b_exp)) {
				b_is_null_found = true;
			} else {
				valid = false;
				break;
			}
		} else {
			valid = false;
			break;
		}
	}
	if (valid && a_is_null_found && b_is_null_found) {
		return make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, move(equal_cast->left),
		                                              move(equal_cast->right));
	}
	return nullptr;
}

unique_ptr<Expression> EqualOrNullSimplification::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                        bool &changes_made, bool is_root) {
	const Expression *or_exp = bindings[0];

	if (or_exp->type != ExpressionType::CONJUNCTION_OR) {
		return nullptr;
	}

	const auto or_exp_cast = (BoundConjunctionExpression *)or_exp;

	if (or_exp_cast->children.size() != 2) {
		return nullptr;
	}

	const auto left_exp = or_exp_cast->children[0].get();
	const auto right_exp = or_exp_cast->children[1].get();
	// Test for: a=b OR (a IS NULL AND b IS NULL)
	auto first_try = TryRewriteEqualOrIsNull(left_exp, right_exp);
	if (first_try) {
		return first_try;
	}
	// Test for: (a IS NULL AND b IS NULL) OR a=b
	return TryRewriteEqualOrIsNull(right_exp, left_exp);
}

} // namespace duckdb
