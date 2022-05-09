#include "duckdb/optimizer/rule/equal_or_null_simplification.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

EqualOrNullSimplification::EqualOrNullSimplification(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on OR conjunction
	root = make_unique<ExpressionMatcher>();
	root->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::CONJUNCTION_OR);
}

// a=b OR (a IS NULL AND b IS NULL) to a IS NOT DISTINCT FROM b
static unique_ptr<Expression> TryRewriteEqualOrIsNull(const Expression *e1, const Expression *e2) {
	if (e1->type == ExpressionType::COMPARE_EQUAL && e2->type == ExpressionType::CONJUNCTION_AND &&
	    ((BoundConjunctionExpression *)e2)->children.size() == 2) {
		const auto equal_comp = (BoundComparisonExpression *)e1;
		const auto a_exp = equal_comp->left.get(), b_exp = equal_comp->right.get();
		bool valid = true, a_is_null_found = false, b_is_null_found = false;

		// Make sure on the AND conjuction the relevant conditions appear
		for (const auto &item : ((BoundConjunctionExpression *)e2)->children) {
			const auto next_exp = item.get();

			if (next_exp->type == ExpressionType::OPERATOR_IS_NULL) {
				const auto child = ((BoundOperatorExpression *)next_exp)->children[0].get();
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
			return make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
			                                              move(equal_comp->left), move(equal_comp->right));
		}
	}
	return nullptr;
}

unique_ptr<Expression> EqualOrNullSimplification::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                        bool &changes_made, bool is_root) {
	const auto or_exp = bindings[0];

	if (((BoundConjunctionExpression *)or_exp)->children.size() == 2) {
		const auto or_cast = (BoundConjunctionExpression *)or_exp;
		const auto left_exp = or_cast->children[0].get(), right_exp = or_cast->children[1].get();
		// Test for: a=b OR (a IS NULL AND b IS NULL)
		auto one_try = TryRewriteEqualOrIsNull(left_exp, right_exp);
		if (one_try != nullptr) {
			return one_try;
		}
		// Test for: (a IS NULL AND b IS NULL) OR a=b
		return TryRewriteEqualOrIsNull(right_exp, left_exp);
	}
	return nullptr;
}

} // namespace duckdb
