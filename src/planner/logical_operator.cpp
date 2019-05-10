#include "planner/logical_operator.hpp"

#include "common/printer.hpp"

using namespace duckdb;
using namespace std;

string LogicalOperator::ParamsToString() const {
	string result = "";
	if (expressions.size() > 0) {
		result += "[";
		for (uint64_t i = 0; i < expressions.size(); i++) {
			auto &child = expressions[i];
			result += child->GetName();
			if (i < expressions.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}

void LogicalOperator::ResolveOperatorTypes() {
	// if (types.size() > 0) {
	// 	// types already resolved for this node
	// 	return;
	// }
	types.clear();
	// first resolve child types
	for (auto &child : children) {
		child->ResolveOperatorTypes();
	}
	// now resolve the types for this operator
	ResolveTypes();
}

string LogicalOperator::ToString(uint64_t depth) const {
	string result = LogicalOperatorToString(type);
	result += ParamsToString();
	if (children.size() > 0) {
		for (uint64_t i = 0; i < children.size(); i++) {
			result += "\n" + string(depth * 4, ' ');
			auto &child = children[i];
			result += child->ToString(depth + 1);
		}
		result += "";
	}
	return result;
}

uint64_t LogicalOperator::ExpressionCount() {
	return expressions.size();
}

Expression *LogicalOperator::GetExpression(uint64_t index) {
	assert(index < expressions.size());
	return expressions[index].get();
}

void LogicalOperator::ReplaceExpression(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, uint64_t index) {
	assert(index < expressions.size());
	expressions[index] = callback(move(expressions[index]));
}

void LogicalOperator::Print() {
	Printer::Print(ToString());
}
