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

uint64_t LogicalAggregate::ExpressionCount() {
	return expressions.size() + groups.size();
}

Expression *LogicalAggregate::GetExpression(uint64_t index) {
	if (index < expressions.size()) {
		return LogicalOperator::GetExpression(index);
	} else {
		index -= expressions.size();
		assert(index < groups.size());
		return groups[index].get();
	}
}

void LogicalAggregate::ReplaceExpression(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, uint64_t index) {
	if (index < expressions.size()) {
		LogicalOperator::ReplaceExpression(callback, index);
	} else {
		index -= expressions.size();
		assert(index < groups.size());
		groups[index] = callback(move(groups[index]));
	}
}

string LogicalAggregate::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (groups.size() > 0) {
		result += "[";
		for (uint64_t i = 0; i < groups.size(); i++) {
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
