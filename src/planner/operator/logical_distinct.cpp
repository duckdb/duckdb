#include "planner/operator/logical_distinct.hpp"

using namespace duckdb;
using namespace std;

using namespace duckdb;
using namespace std;

/*void LogicalDistinct::ResolveTypes() {
    for (auto &expr : distinct_targets) {
        types.push_back(expr->return_type);
    }
    // get the chunk types from the projection list
    for (auto &expr : expressions) {
        types.push_back(expr->return_type);
    }
}*/

count_t LogicalDistinct::ExpressionCount() {
	return expressions.size() + distinct_targets.size();
}

Expression *LogicalDistinct::GetExpression(index_t index) {
	if (index < expressions.size()) {
		return LogicalOperator::GetExpression(index);
	} else {
		index -= expressions.size();
		assert(index < distinct_targets.size());
		return distinct_targets[index].get();
	}
}

void LogicalDistinct::ReplaceExpression(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, index_t index) {
	if (index < expressions.size()) {
		LogicalOperator::ReplaceExpression(callback, index);
	} else {
		index -= expressions.size();
		assert(index < distinct_targets.size());
		distinct_targets[index] = callback(move(distinct_targets[index]));
	}
}

string LogicalDistinct::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (distinct_targets.size() > 0) {
		result += "[";
		for (index_t i = 0; i < distinct_targets.size(); i++) {
			auto &child = distinct_targets[i];
			result += child->GetName();
			if (i < distinct_targets.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}
