#include "planner/logical_operator.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
bool IsProjection(LogicalOperatorType type) {
	return type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY || type == LogicalOperatorType::PROJECTION;
}

LogicalOperator *GetProjection(LogicalOperator *node) {
	while ((node->children.size() == 1 || node->type == LogicalOperatorType::UNION ||
	        node->type == LogicalOperatorType::EXCEPT || node->type == LogicalOperatorType::INTERSECT) &&
	       !IsProjection(node->type)) {
		node = node->children[0].get();
	}
	return node;
}

} // namespace duckdb

string LogicalOperator::ParamsToString() const {
	string result = "";
	if (expressions.size() > 0) {
		result += "[";
		for (size_t i = 0; i < expressions.size(); i++) {
			auto &child = expressions[i];
			result += child->ToString();
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

string LogicalOperator::ToString(size_t depth) const {
	string result = LogicalOperatorToString(type);
	result += ParamsToString();
	if (children.size() > 0) {
		for (size_t i = 0; i < children.size(); i++) {
			result += "\n" + string(depth * 4, ' ');
			auto &child = children[i];
			result += child->ToString(depth + 1);
		}
		result += "";
	}
	return result;
}

size_t LogicalOperator::ExpressionCount() {
	return expressions.size();
}

Expression *LogicalOperator::GetExpression(size_t index) {
	assert(index < expressions.size());
	return expressions[index].get();
}

void LogicalOperator::ReplaceExpression(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, size_t index) {
	assert(index < expressions.size());
	expressions[index] = callback(move(expressions[index]));
}
