
#include <algorithm>
#include <vector>

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "common/value_operations/value_operations.hpp"
#include "optimizer/rule.hpp"

#include "optimizer/expression_rules/distributivity.hpp"

using namespace duckdb;
using namespace std;

// match on (A AND B) OR (C AND D)
DistributivityRule::DistributivityRule() {
	root = make_unique<ExpressionNodeType>(ExpressionType::CONJUNCTION_OR);
	root->child_policy = ChildPolicy::UNORDERED;

	root->children.push_back(
	    make_unique<ExpressionNodeType>(ExpressionType::CONJUNCTION_AND));
	root->children.push_back(
	    make_unique<ExpressionNodeType>(ExpressionType::CONJUNCTION_AND));
	root->child_policy = ChildPolicy::UNORDERED;
}

std::unique_ptr<Expression>
DistributivityRule::Apply(Rewriter &rewriter, Expression &root,
                          std::vector<AbstractOperator> &bindings,
                          bool &fixed_point) {
	auto main_or = (ConjunctionExpression *)bindings[0].value.expr;
	auto land = (ConjunctionExpression *)bindings[1].value.expr;
	auto rand = (ConjunctionExpression *)bindings[2].value.expr;
	assert(main_or->children.size() == 2);
	assert(land->children.size() == 2);
	assert(rand->children.size() == 2);

	int left = -1, right = -1;
	if (land->children[0]->Equals(rand->children[0].get())) {
		left = 0;
		right = 0;
	} else if (land->children[0]->Equals(rand->children[1].get())) {
		left = 0;
		right = 1;
	} else if (land->children[1]->Equals(rand->children[0].get())) {
		left = 1;
		right = 0;
	} else if (land->children[1]->Equals(rand->children[1].get())) {
		left = 1;
		right = 1;
	}
	if (left < 0) {
		// none of the expressions were equal
		return nullptr;
	}
	// left == right, rewrite to left AND (x OR y)
	// we just take the left AND expression and make it the new root
	auto new_root = move(main_or->children[0]);
	// now set up the children of the OR expression to the two other expressions
	// that were not equal
	auto or_left = move(land->children[1 - left]);
	auto or_right = move(rand->children[1 - right]);
	// now create the OR expression over the child we just took
	land->children[1 - left] = make_unique<ConjunctionExpression>(
	    ExpressionType::CONJUNCTION_OR, move(or_left), move(or_right));
	// and return the new root
	return new_root;
};