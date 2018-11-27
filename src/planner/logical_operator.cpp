
#include "planner/logical_operator.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
bool IsProjection(LogicalOperatorType type) {
	return type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY ||
	       type == LogicalOperatorType::PROJECTION;
}

LogicalOperator *GetProjection(LogicalOperator *node) {
	while ((node->children.size() == 1 ||
	        node->type == LogicalOperatorType::UNION ||
	        node->type == LogicalOperatorType::EXCEPT ||
	        node->type == LogicalOperatorType::INTERSECT) &&
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

string LogicalOperator::ToString() const {
	string result = LogicalOperatorToString(type);
	result += ParamsToString();
	if (children.size() > 0) {
		result += "(";
		for (size_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			result += child->ToString();
			if (i < children.size() - 1) {
				result += ", ";
			}
		}
		result += ")";
	}
	return result;
}

size_t LogicalOperator::ExpressionCount() {
	return expressions.size();
}

Expression *LogicalOperator::GetExpression(size_t index) {
	if (index >= ExpressionCount()) {
		throw OutOfRangeException(
		    "GetExpression(): Expression index out of range!");
	}
	return expressions[index].get();
}

void LogicalOperator::SetExpression(size_t index,
                                    std::unique_ptr<Expression> expr) {
	if (index >= ExpressionCount()) {
		throw OutOfRangeException(
		    "SetExpression(): Expression index out of range!");
	}
	expressions[index] = std::move(expr);
}
