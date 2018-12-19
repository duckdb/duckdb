#include "planner/operator/logical_aggregate.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalAggregate::GetNames() {
	vector<string> names;
	for (auto &expr : groups) {
		names.push_back(expr->GetName());
	}
	for (auto &exp : expressions) {
		names.push_back(exp->GetName());
	}
	return names;
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

size_t LogicalAggregate::ExpressionCount() {
	return expressions.size() + groups.size();
}

Expression *LogicalAggregate::GetExpression(size_t index) {
	if (index >= ExpressionCount()) {
		throw OutOfRangeException("GetExpression(): Expression index out of range!");
	}
	if (index >= expressions.size()) {
		return groups[index - expressions.size()].get();
	}
	return expressions[index].get();
}

void LogicalAggregate::SetExpression(size_t index, unique_ptr<Expression> expr) {
	if (index >= ExpressionCount()) {
		throw OutOfRangeException("SetExpression(): Expression index out of range!");
	}
	if (index >= expressions.size()) {
		groups[index - expressions.size()] = move(expr);
	} else {
		expressions[index] = move(expr);
	}
}

string LogicalAggregate::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (groups.size() > 0) {
		result += "[";
		for (size_t i = 0; i < groups.size(); i++) {
			auto &child = groups[i];
			result += child->ToString();
			if (i < groups.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}
