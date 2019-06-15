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
		for (index_t i = 0; i < conditions.size(); i++) {
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
