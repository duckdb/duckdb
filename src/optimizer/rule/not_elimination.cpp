#include "duckdb/optimizer/rule/not_elimination.hpp"

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

namespace {

class RecursiveConjunctionExpressionMatcher : public ExpressionMatcher {
public:
	explicit RecursiveConjunctionExpressionMatcher(unique_ptr<ExpressionMatcher> matcher)
	    : ExpressionMatcher(ExpressionClass::BOUND_CONJUNCTION), matcher(std::move(matcher)) {
	}
	bool Match(Expression &expr_p, vector<reference<Expression>> &bindings) override {
		if (RecursiveMatch(expr_p, bindings)) {
			bindings.clear();
			bindings.push_back(expr_p);
			return true;
		}
		return false;
	}

private:
	bool RecursiveMatch(Expression &expr_p, vector<reference<Expression>> &bindings) {
		if (!matcher->Match(expr_p, bindings)) {
			return false;
		}

		auto &expr = expr_p.Cast<BoundConjunctionExpression>();
		for (auto &child : expr.children) {
			if (child->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
				if (!RecursiveMatch(*child, bindings)) {
					return false;
				}
			}
		}

		return true;
	}

	unique_ptr<ExpressionMatcher> matcher;
};

class NotExpressionMatcher : public ExpressionMatcher {
public:
	NotExpressionMatcher() : ExpressionMatcher(ExpressionClass::BOUND_OPERATOR) {
	}
	// The matchers for the child expressions.
	vector<unique_ptr<ExpressionMatcher>> matchers;

	bool Match(Expression &expr_p, vector<reference<Expression>> &bindings) override {
		if (!ExpressionMatcher::Match(expr_p, bindings)) {
			return false;
		}
		auto &expr = expr_p.Cast<BoundOperatorExpression>();
		for (auto &child : expr.children) {
			for (auto &matcher : matchers) {
				if (matcher->Match(*child, bindings)) {
					bindings.clear();
					bindings.push_back(expr);
					return true;
				}
			}
		}
		return false;
	}
};

} // namespace

NotEliminationRule::NotEliminationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// Matcher for child NOT.
	// eg. NOT NOT Expression.
	auto child_not_matcher = make_uniq<ExpressionMatcher>();
	child_not_matcher->expr_class = ExpressionClass::BOUND_OPERATOR;
	child_not_matcher->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::OPERATOR_NOT);

	// Matcher for child IS_NULL/IS_NOT_NULL.
	// eg. NOT IS_NULL/IS_NOT_NULL
	auto child_null_matcher = make_uniq<ExpressionMatcher>();
	child_null_matcher->expr_class = ExpressionClass::BOUND_OPERATOR;
	child_null_matcher->expr_type = make_uniq<ManyExpressionTypeMatcher>(
	    vector<ExpressionType> {ExpressionType::OPERATOR_IS_NOT_NULL, ExpressionType::OPERATOR_IS_NULL});

	// Matcher for child AND/OR conjunction.
	// eg. NOT (col1 > 3 AND (col3 > 0 AND col2 < 2))
	static const vector<ExpressionType> valid_types = {
	    ExpressionType::COMPARE_EQUAL,
	    ExpressionType::COMPARE_NOTEQUAL,
	    ExpressionType::COMPARE_LESSTHAN,
	    ExpressionType::COMPARE_GREATERTHAN,
	    ExpressionType::COMPARE_LESSTHANOREQUALTO,
	    ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	    ExpressionType::COMPARE_DISTINCT_FROM,
	    ExpressionType::COMPARE_NOT_DISTINCT_FROM,
	    ExpressionType::CONJUNCTION_AND,
	    ExpressionType::CONJUNCTION_OR,
	};
	auto expr_matcher = make_uniq<ExpressionMatcher>();
	expr_matcher->expr_type = make_uniq<ManyExpressionTypeMatcher>(valid_types);
	auto child_conjunction_matcher = make_uniq<RecursiveConjunctionExpressionMatcher>(std::move(expr_matcher));

	// Matcher for root NOT.
	auto not_matcher = make_uniq<NotExpressionMatcher>();
	not_matcher->expr_class = ExpressionClass::BOUND_OPERATOR;
	not_matcher->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::OPERATOR_NOT);
	not_matcher->matchers.push_back(std::move(child_not_matcher));
	not_matcher->matchers.push_back(std::move(child_null_matcher));
	not_matcher->matchers.push_back(std::move(child_conjunction_matcher));
	root = std::move(not_matcher);
}

unique_ptr<Expression> NotEliminationRule::NegateExpression(const Expression &expr) {
	static const unordered_map<ExpressionType, ExpressionType, ExpressionTypeHash> negate_types = {
	    {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_NOTEQUAL},
	    {ExpressionType::COMPARE_NOTEQUAL, ExpressionType::COMPARE_EQUAL},
	    {ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_GREATERTHANOREQUALTO},
	    {ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_LESSTHANOREQUALTO},
	    {ExpressionType::COMPARE_LESSTHANOREQUALTO, ExpressionType::COMPARE_GREATERTHAN},
	    {ExpressionType::COMPARE_GREATERTHANOREQUALTO, ExpressionType::COMPARE_LESSTHAN},
	    {ExpressionType::COMPARE_DISTINCT_FROM, ExpressionType::COMPARE_NOT_DISTINCT_FROM},
	    {ExpressionType::COMPARE_NOT_DISTINCT_FROM, ExpressionType::COMPARE_DISTINCT_FROM},
	    {ExpressionType::OPERATOR_IS_NULL, ExpressionType::OPERATOR_IS_NOT_NULL},
	    {ExpressionType::OPERATOR_IS_NOT_NULL, ExpressionType::OPERATOR_IS_NULL},
	    {ExpressionType::CONJUNCTION_OR, ExpressionType::OPERATOR_NOT},
	    {ExpressionType::CONJUNCTION_AND, ExpressionType::OPERATOR_NOT},
	};

	auto negate_type_iter = negate_types.find(expr.GetExpressionType());
	if (DUCKDB_UNLIKELY(negate_type_iter == negate_types.end())) {
		return nullptr;
	}

	switch (expr.GetExpressionType()) {
	case ExpressionType::CONJUNCTION_OR:
	case ExpressionType::CONJUNCTION_AND: {
		auto not_expr = make_uniq<BoundOperatorExpression>(negate_type_iter->second, expr.return_type);
		not_expr->children.push_back(expr.Copy());
		return std::move(not_expr);
	}
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL: {
		const auto &op_expr = expr.Cast<BoundOperatorExpression>();
		auto negate_op_expr = make_uniq<BoundOperatorExpression>(negate_type_iter->second, LogicalType::BOOLEAN);
		negate_op_expr->children.push_back(op_expr.children[0]->Copy());
		return std::move(negate_op_expr);
	}
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM: {
		const auto &compare_expr = expr.Cast<BoundComparisonExpression>();
		return make_uniq<BoundComparisonExpression>(negate_type_iter->second, compare_expr.left->Copy(),
		                                            compare_expr.right->Copy());
	}
	default:
		return nullptr;
	}
}

unique_ptr<Expression> NotEliminationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                 bool &changes_made, bool is_root) {
	auto &root = bindings.back().get().Cast<BoundOperatorExpression>();
	D_ASSERT(root.GetExpressionType() == ExpressionType::OPERATOR_NOT);
	D_ASSERT(root.children.size() == 1);
	auto &child = *(root.children[0].get());

	if (child.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &child_op = child.Cast<BoundOperatorExpression>();
		D_ASSERT(child_op.children.size() == 1);
		// NOT NOT Expression ==> Expression.
		if (child.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
			return child_op.children[0]->Copy();
		} else {
			// NOT IS_NULL/IS_NOT_NULL ===>  IS_NOT_NULL/IS_NULL.
			D_ASSERT(child.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
			         child.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL);
			return NegateExpression(child);
		}
	}

	D_ASSERT(child.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION);
	auto &conjunction_child = child.Cast<BoundConjunctionExpression>();
	vector<unique_ptr<Expression>> new_children;
	for (const auto &expr : conjunction_child.children) {
		auto neg_expr = NegateExpression(*expr);
		if (neg_expr == nullptr) {
			return nullptr;
		}
		new_children.push_back(std::move(neg_expr));
	}

	// NOT (Expression1 AND Expression2) ==> !Expression1 OR !Expression2
	if (conjunction_child.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto comp = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
		comp->children = std::move(new_children);
		return std::move(comp);

	} else {
		// NOT (Expression1 OR Expression2) ==> !Expression1 AND !Expression2
		D_ASSERT(conjunction_child.GetExpressionType() == ExpressionType::CONJUNCTION_OR);
		auto comp = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		comp->children = std::move(new_children);
		return std::move(comp);
	}
}

} // namespace duckdb
