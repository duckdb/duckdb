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

static void GatherAndExpressions(Expression *expression, vector<Expression *> &result) {
	if (expression->type == ExpressionType::CONJUNCTION_AND) {
		// gather expressions
		ExpressionIterator::EnumerateChildren(*expression,
		                                      [&](Expression &child) { GatherAndExpressions(&child, result); });
	} else {
		// just add the expression
		result.push_back(expression);
	}
}

static void GatherOrExpressions(Expression *expression, vector<vector<Expression *>> &result) {
	assert(expression->type == ExpressionType::CONJUNCTION_OR);
	// traverse the children
	ExpressionIterator::EnumerateChildren(*expression, [&](Expression &child) {
		if (child.type == ExpressionType::CONJUNCTION_OR) {
			GatherOrExpressions(&child, result);
		} else {
			vector<Expression *> new_expressions;
			GatherAndExpressions(&child, new_expressions);
			result.push_back(new_expressions);
		}
	});
}

static unique_ptr<Expression> Prune(unique_ptr<Expression> root) {
	if (root->type == ExpressionType::INVALID) {
		// prune this node
		return nullptr;
	}
	if (root->type == ExpressionType::CONJUNCTION_OR || root->type == ExpressionType::CONJUNCTION_AND) {
		auto &conj = (BoundConjunctionExpression &)*root;
		// conjunction, prune recursively
		conj.left = Prune(move(conj.left));
		conj.right = Prune(move(conj.right));
		if (conj.left && conj.right) {
			// don't prune
			return root;
		} else if (conj.left) {
			// prune right
			return move(conj.left);
		} else if (conj.right) {
			// prune left
			return move(conj.right);
		} else {
			// prune entire node
			return nullptr;
		}
	}
	// no conjunction or invalid, just return the node again
	return root;
}

unique_ptr<Expression> DistributivityRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                 bool &changes_made) {
	auto initial_or = (BoundConjunctionExpression *)bindings[0];
	// gather all the expressions inside AND expressions
	vector<vector<Expression *>> gathered_expressions;
	GatherOrExpressions(initial_or, gathered_expressions);

	// now we have a list of expressions we have gathered for this OR
	// if every list in this OR contains the same expression, we can extract
	// that expression
	// FIXME: this could be done more efficiently with a hashmap

	vector<int> matches;
	matches.resize(gathered_expressions.size());

	unique_ptr<Expression> new_root;
	for (index_t i = 0; i < gathered_expressions[0].size(); i++) {
		auto entry = gathered_expressions[0][i];

		matches[0] = i;
		bool occurs_in_all_expressions = true;
		for (index_t j = 1; j < gathered_expressions.size(); j++) {
			matches[j] = -1;
			for (index_t k = 0; k < gathered_expressions[j].size(); k++) {
				auto other_entry = gathered_expressions[j][k];
				if (Expression::Equals(entry, other_entry)) {
					// match found
					matches[j] = k;
					break;
				}
			}
			if (matches[j] < 0) {
				occurs_in_all_expressions = false;
				break;
			}
		}
		if (occurs_in_all_expressions) {
			assert(matches.size() >= 2);
			// this expression occurs in all expressions, we can push it up
			// before the main OR expression.

			// make a copy of the right child for usage in the root
			auto right_child = gathered_expressions[0][i]->Copy();

			// now we need to remove each matched entry from its parents.
			// for all nodes, set the ExpressionType to INVALID
			// we do the actual pruning later
			for (index_t m = 0; m < matches.size(); m++) {
				auto entry = gathered_expressions[m][matches[m]];
				entry->type = ExpressionType::INVALID;
			}

			unique_ptr<Expression> left_child;
			if (new_root) {
				// we already have a new root, set that as the left child
				left_child = move(new_root);
			} else {
				// no new root yet, create a new OR expression with the children
				// of the main root
				left_child = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR,
				                                                     move(initial_or->left), move(initial_or->right));
			}
			new_root = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(left_child),
			                                                   move(right_child));
		}
	}
	if (new_root) {
		// we made a new root
		// we need to prune invalid entries!
		new_root = Prune(move(new_root));
		assert(new_root);
		return new_root;
	} else {
		return nullptr;
	}
}
