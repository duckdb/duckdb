#include "duckdb/optimizer/rule/distributivity.hpp"

#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

DistributivityRule::DistributivityRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// we match on an OR expression within a LogicalFilter node
	root = make_unique<ExpressionMatcher>();
	root->expr_type = make_unique<SpecificExpressionTypeMatcher>(ExpressionType::CONJUNCTION_OR);
}

void DistributivityRule::AddExpressionSet(Expression &expr, expression_set_t &set) {
	if (expr.type == ExpressionType::CONJUNCTION_AND) {
		auto &and_expr = (BoundConjunctionExpression &)expr;
		for (auto &child : and_expr.children) {
			set.insert(child.get());
		}
	} else {
		set.insert(&expr);
	}
}

unique_ptr<Expression> DistributivityRule::ExtractExpression(BoundConjunctionExpression &conj, idx_t idx,
                                                             Expression &expr) {
	auto &child = conj.children[idx];
	unique_ptr<Expression> result;
	if (child->type == ExpressionType::CONJUNCTION_AND) {
		// AND, remove expression from the list
		auto &and_expr = (BoundConjunctionExpression &)*child;
		for (idx_t i = 0; i < and_expr.children.size(); i++) {
			if (Expression::Equals(and_expr.children[i].get(), &expr)) {
				result = move(and_expr.children[i]);
				and_expr.children.erase(and_expr.children.begin() + i);
				break;
			}
		}
		if (and_expr.children.size() == 1) {
			conj.children[idx] = move(and_expr.children[0]);
		}
	} else {
		// not an AND node! remove the entire expression
		// this happens in the case of e.g. (X AND B) OR X
		assert(Expression::Equals(child.get(), &expr));
		result = move(child);
		conj.children[idx] = nullptr;
	}
	assert(result);
	return result;
}

unique_ptr<Expression> DistributivityRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                 bool &changes_made) {
	auto initial_or = (BoundConjunctionExpression *)bindings[0];

	// we want to find expressions that occur in each of the children of the OR
	// i.e. (X AND A) OR (X AND B) => X occurs in all branches
	// first, for the initial child, we create an expression set of which expressions occur
	// this is our initial candidate set (in the example: [X, A])
	expression_set_t candidate_set;
	AddExpressionSet(*initial_or->children[0], candidate_set);
	// now for each of the remaining children, we create a set again and intersect them
	// in our example: the second set would be [X, B]
	// the intersection would leave [X]
	for (idx_t i = 1; i < initial_or->children.size(); i++) {
		expression_set_t next_set;
		AddExpressionSet(*initial_or->children[i], next_set);
		expression_set_t intersect_result;
		for (auto &expr : candidate_set) {
			if (next_set.find(expr) != next_set.end()) {
				intersect_result.insert(expr);
			}
		}
		candidate_set = intersect_result;
	}
	if (candidate_set.size() == 0) {
		// nothing found: abort
		return nullptr;
	}
	// now for each of the remaining expressions in the candidate set we know that it is contained in all branches of
	// the OR
	auto new_root = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &expr : candidate_set) {
		assert(initial_or->children.size() > 0);

		// extract the expression from the first child of the OR
		auto result = ExtractExpression(*initial_or, 0, (Expression &)*expr);
		// now for the subsequent expressions, simply remove the expression
		for (idx_t i = 1; i < initial_or->children.size(); i++) {
			ExtractExpression(*initial_or, i, *result);
		}
		// now we add the expression to the new root
		new_root->children.push_back(move(result));
		// remove any expressions that were set to nullptr
		for (idx_t i = 0; i < initial_or->children.size(); i++) {
			if (!initial_or->children[i]) {
				initial_or->children.erase(initial_or->children.begin() + i);
				i--;
			}
		}
	}
	// finally we need to add the remaining expressions in the OR to the new root
	if (initial_or->children.size() == 1) {
		// one child: skip the OR entirely and only add the single child
		new_root->children.push_back(move(initial_or->children[0]));
	} else if (initial_or->children.size() > 1) {
		// multiple children still remain: push them into a new OR and add that to the new root
		auto new_or = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
		for (auto &child : initial_or->children) {
			new_or->children.push_back(move(child));
		}
		new_root->children.push_back(move(new_or));
	}
	// finally return the new root
	if (new_root->children.size() == 1) {
		return move(new_root->children[0]);
	}
	return move(new_root);
}
