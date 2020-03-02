#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

LogicalAggregate::LogicalAggregate(idx_t group_index, idx_t aggregate_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY, move(select_list)), group_index(group_index),
      aggregate_index(aggregate_index) {
}

void LogicalAggregate::ResolveTypes() {
	for (auto &expr : groups) {
		types.push_back(expr->return_type);
	}
	// get the chunk types from the projection list
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<ColumnBinding> LogicalAggregate::GetColumnBindings() {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < groups.size(); i++) {
		result.push_back(ColumnBinding(group_index, i));
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		result.push_back(ColumnBinding(aggregate_index, i));
	}
	return result;
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
