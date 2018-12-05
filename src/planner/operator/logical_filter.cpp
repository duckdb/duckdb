#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

LogicalFilter::LogicalFilter(unique_ptr<Expression> expression) : LogicalOperator(LogicalOperatorType::FILTER) {
	expressions.push_back(move(expression));
	SplitPredicates();
}

LogicalFilter::LogicalFilter() : LogicalOperator(LogicalOperatorType::FILTER) {
}

vector<string> LogicalFilter::GetNames() {
	return children[0]->GetNames();
}

void LogicalFilter::ResolveTypes() {
	types = children[0]->types;
}

// Split the predicates separated by AND statements
// These are the predicates that are safe to push down because all of them MUST
// be true
bool LogicalFilter::SplitPredicates() {
	bool found_conjunction = false;
	for (size_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->type == ExpressionType::CONJUNCTION_AND) {
			found_conjunction = true;
			// AND expression, split into left and right child
			expressions.push_back(move(expressions[i]->children[0]));
			expressions[i] = move(expressions[i]->children[1]);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
		}
	}
	return found_conjunction;
}
