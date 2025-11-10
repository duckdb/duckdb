#include "duckdb/optimizer/rule/equal_or_null_simplification.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

EqualOrNullSimplification::EqualOrNullSimplification(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on OR conjunction
	auto op = make_uniq<ConjunctionExpressionMatcher>();
	op->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::CONJUNCTION_OR);
	op->policy = SetMatcher::Policy::SOME;

	// equi comparison on one side
	auto equal_child = make_uniq<ComparisonExpressionMatcher>();
	equal_child->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);
	equal_child->policy = SetMatcher::Policy::SOME;
	op->matchers.push_back(std::move(equal_child));

	// AND conjunction on the other
	auto and_child = make_uniq<ConjunctionExpressionMatcher>();
	and_child->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::CONJUNCTION_AND);
	and_child->policy = SetMatcher::Policy::SOME;

	// IS NULL tests inside AND
	auto isnull_child = make_uniq<ExpressionMatcher>();
	isnull_child->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::OPERATOR_IS_NULL);
	// I could try to use std::make_uniq for a copy, but it's available from C++14 only
	auto isnull_child2 = make_uniq<ExpressionMatcher>();
	isnull_child2->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::OPERATOR_IS_NULL);
	and_child->matchers.push_back(std::move(isnull_child));
	and_child->matchers.push_back(std::move(isnull_child2));

	op->matchers.push_back(std::move(and_child));
	root = std::move(op);
}

// a=b OR (a IS NULL AND b IS NULL) to a IS NOT DISTINCT FROM b
static unique_ptr<Expression> TryRewriteEqualOrIsNull(Expression &equal_expr, Expression &and_expr) {
	if (equal_expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL ||
	    and_expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
		return nullptr;
	}

	auto &equal_cast = equal_expr.Cast<BoundComparisonExpression>();
	auto &and_cast = and_expr.Cast<BoundConjunctionExpression>();

	if (and_cast.children.size() != 2) {
		return nullptr;
	}

	// Make sure on the AND conjunction the relevant conditions appear
	auto &a_exp = *equal_cast.left;
	auto &b_exp = *equal_cast.right;
	bool a_is_null_found = false;
	bool b_is_null_found = false;

	for (const auto &item : and_cast.children) {
		auto &next_exp = *item;

		if (next_exp.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
			auto &next_exp_cast = next_exp.Cast<BoundOperatorExpression>();
			auto &child = *next_exp_cast.children[0];

			// Test for equality on both 'a' and 'b' expressions
			if (Expression::Equals(child, a_exp)) {
				a_is_null_found = true;
			} else if (Expression::Equals(child, b_exp)) {
				b_is_null_found = true;
			} else {
				return nullptr;
			}
		} else {
			return nullptr;
		}
	}
	if (a_is_null_found && b_is_null_found) {
		return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
		                                            std::move(equal_cast.left), std::move(equal_cast.right));
	}
	return nullptr;
}

unique_ptr<Expression> EqualOrNullSimplification::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                        bool &changes_made, bool is_root) {
	const Expression &or_exp = bindings[0].get();

	if (or_exp.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return nullptr;
	}

	const auto &or_exp_cast = or_exp.Cast<BoundConjunctionExpression>();

	if (or_exp_cast.children.size() != 2) {
		return nullptr;
	}

	auto &left_exp = *or_exp_cast.children[0];
	auto &right_exp = *or_exp_cast.children[1];
	// Test for: a=b OR (a IS NULL AND b IS NULL)
	auto first_try = TryRewriteEqualOrIsNull(left_exp, right_exp);
	if (first_try) {
		return first_try;
	}
	// Test for: (a IS NULL AND b IS NULL) OR a=b
	return TryRewriteEqualOrIsNull(right_exp, left_exp);
}

} // namespace duckdb
