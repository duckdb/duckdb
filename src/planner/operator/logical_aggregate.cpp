#include "planner/operator/logical_aggregate.hpp"

using namespace duckdb;
using namespace std;

void LogicalAggregate::ResolveTypes() {
	for (auto &expr : groups) {
		types.push_back(expr->return_type);
	}
	// get the chunk types from the projection list
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

string LogicalAggregate::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (groups.size() > 0) {
		result += "[";
		for (index_t i = 0; i < groups.size(); i++) {
			auto &child = groups[i];
			result += child->GetName();
			if (i < groups.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}
