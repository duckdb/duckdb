#include "planner/operator/logical_comparison_join.hpp"

using namespace duckdb;
using namespace std;

LogicalComparisonJoin::LogicalComparisonJoin(JoinType type, LogicalOperatorType logical_type)
    : LogicalJoin(type, logical_type) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = "";
	if (conditions.size() > 0) {
		result += "[";
		for (uint64_t i = 0; i < conditions.size(); i++) {
			auto &cond = conditions[i];
			result += ExpressionTypeToString(cond.comparison) + "(" + cond.left->GetName() + ", " +
			          cond.right->GetName() + ")";
			if (i < conditions.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}

uint64_t LogicalComparisonJoin::ExpressionCount() {
	assert(expressions.size() == 0);
	return conditions.size() * 2;
}

Expression *LogicalComparisonJoin::GetExpression(uint64_t index) {
	assert(expressions.size() == 0);
	assert(index < conditions.size() * 2);
	uint64_t condition = index / 2;
	bool left = index % 2 == 0 ? true : false;
	assert(condition < conditions.size());
	return left ? conditions[condition].left.get() : conditions[condition].right.get();
}

void LogicalComparisonJoin::ReplaceExpression(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, uint64_t index) {
	assert(expressions.size() == 0);
	assert(index < conditions.size() * 2);
	uint64_t condition = index / 2;
	bool left = index % 2 == 0 ? true : false;
	assert(condition < conditions.size());
	if (left) {
		conditions[condition].left = callback(move(conditions[condition].left));
	} else {
		conditions[condition].right = callback(move(conditions[condition].right));
	}
}
