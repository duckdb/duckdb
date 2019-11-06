#include "duckdb/planner/operator/logical_filter.hpp"

#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

LogicalFilter::LogicalFilter(unique_ptr<Expression> expression) : LogicalOperator(LogicalOperatorType::FILTER) {
	expressions.push_back(move(expression));
	SplitPredicates(expressions);
}

LogicalFilter::LogicalFilter() : LogicalOperator(LogicalOperatorType::FILTER) {
}

void LogicalFilter::ResolveTypes() {
	types = children[0]->types;
}

// Split the predicates separated by AND statements
// These are the predicates that are safe to push down because all of them MUST
// be true
bool LogicalFilter::SplitPredicates(vector<unique_ptr<Expression>> &expressions) {
	bool found_conjunction = false;
	for (index_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->type == ExpressionType::CONJUNCTION_AND) {
			auto &conjunction = (BoundConjunctionExpression &)*expressions[i];
			found_conjunction = true;
			// AND expression, split into left and right child
			expressions.push_back(move(conjunction.left));
			expressions[i] = move(conjunction.right);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
		}
	}
	return found_conjunction;
}
