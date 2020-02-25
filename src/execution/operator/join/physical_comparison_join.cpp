#include "duckdb/execution/operator/join/physical_comparison_join.hpp"

using namespace duckdb;
using namespace std;

PhysicalComparisonJoin::PhysicalComparisonJoin(LogicalOperator &op, PhysicalOperatorType type,
                                               vector<JoinCondition> conditions_, JoinType join_type)
    : PhysicalJoin(op, type, join_type) {
	conditions.resize(conditions_.size());
	// we reorder conditions so the ones with COMPARE_EQUAL occur first
	idx_t equal_position = 0;
	idx_t other_position = conditions_.size() - 1;
	for (idx_t i = 0; i < conditions_.size(); i++) {
		if (conditions_[i].comparison == ExpressionType::COMPARE_EQUAL) {
			// COMPARE_EQUAL, move to the start
			conditions[equal_position++] = std::move(conditions_[i]);
		} else {
			// other expression, move to the end
			conditions[other_position--] = std::move(conditions_[i]);
		}
	}
}

string PhysicalComparisonJoin::ExtraRenderInformation() const {
	string extra_info = JoinTypeToString(type) + "\n";
	for (auto &it : conditions) {
		string op = ExpressionTypeToOperator(it.comparison);
		extra_info += it.left->GetName() + op + it.right->GetName() + "\n";
	}
	return extra_info;
}
