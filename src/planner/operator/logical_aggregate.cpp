#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/common/string_util.hpp"

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
		result += StringUtil::Join(groups, groups.size(), ", ",
		                           [](const unique_ptr<Expression> &child) { return child->GetName(); });
		result += "]";
	}

	return result;
}
