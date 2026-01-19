#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

InsertionOrderPreservingMap<string> LogicalComparisonJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToChars(join_type);

	string conditions_info;
	for (idx_t i = 0; i < conditions.size(); i++) {
		if (i > 0) {
			conditions_info += "\n";
		}
		auto &condition = conditions[i];
		if (condition.IsComparison()) {
			auto expr = make_uniq<BoundComparisonExpression>(condition.GetComparisonType(), condition.GetLHS().Copy(),
			                                                 condition.GetRHS().Copy());
			conditions_info += expr->ToString();
		} else {
			conditions_info += condition.GetJoinExpression().ToString();
		}
	}
	result["Conditions"] = conditions_info;
	SetParamsEstimatedCardinality(result);

	return result;
}

bool LogicalComparisonJoin::HasEquality(idx_t &range_count) const {
	bool result = false;
	for (size_t c = 0; c < conditions.size(); ++c) {
		auto &cond = conditions[c];
		if (cond.IsComparison()) {
			switch (cond.GetComparisonType()) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				result = true;
				break;
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				++range_count;
				break;
			case ExpressionType::COMPARE_NOTEQUAL:
			case ExpressionType::COMPARE_DISTINCT_FROM:
				break;
			default:
				throw NotImplementedException("Unimplemented comparison join");
			}
		}
	}
	return result;
}

bool LogicalComparisonJoin::HasArbitraryConditions() const {
	for (size_t c = 0; c < conditions.size(); ++c) {
		auto &cond = conditions[c];
		if (!cond.IsComparison()) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
