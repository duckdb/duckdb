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
	auto child_types = children[0]->types;
	if (projection_map.size() > 0) {
		for(auto index : projection_map) {
			types.push_back(child_types[index]);
		}
	} else {
		types = child_types;
	}
}

vector<ColumnBinding> LogicalFilter::GetColumnBindings() {
	return children[0]->GetColumnBindings();
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
			// AND expression, append the other children
			for (index_t k = 1; k < conjunction.children.size(); k++) {
				expressions.push_back(move(conjunction.children[k]));
			}
			// replace this expression with the first child of the conjunction
			expressions[i] = move(conjunction.children[0]);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
		}
	}
	return found_conjunction;
}
