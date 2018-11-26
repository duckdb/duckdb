
#include "execution/operator/physical_join.hpp"

using namespace duckdb;
using namespace std;

PhysicalJoin::PhysicalJoin(PhysicalOperatorType type,
                           std::vector<JoinCondition> conditions_,
                           JoinType join_type)
    : PhysicalOperator(type), type(join_type) {
	conditions.resize(conditions_.size());
	// we reorder conditions so the ones with COMPARE_EQUAL occur first
	size_t equal_position = 0;
	size_t other_position = conditions_.size() - 1;
	for (size_t i = 0; i < conditions_.size(); i++) {
		if (conditions_[i].comparison == ExpressionType::COMPARE_EQUAL) {
			// COMPARE_EQUAL, move to the start
			conditions[equal_position++] = std::move(conditions_[i]);
		} else {
			// other expression, move to the end
			conditions[other_position--] = std::move(conditions_[i]);
		}
	}
}

vector<string> PhysicalJoin::GetNames() {
	auto left = children[0]->GetNames();
	if (type != JoinType::SEMI && type != JoinType::ANTI) {
		// for normal joins we project both sides
		auto right = children[1]->GetNames();
		left.insert(left.end(), right.begin(), right.end());
	}
	return left;
}

vector<TypeId> PhysicalJoin::GetTypes() {
	auto types = children[0]->GetTypes();
	if (type != JoinType::SEMI && type != JoinType::ANTI) {
		// for normal joins we project both sides
		auto right_types = children[1]->GetTypes();
		types.insert(types.end(), right_types.begin(), right_types.end());
	}
	return types;
}

string PhysicalJoin::ExtraRenderInformation() {
	string extra_info = JoinTypeToString(type) + "\n";
	for (auto &it : conditions) {
		string op = ExpressionTypeToOperator(it.comparison);
		extra_info += it.left->ToString() + op + it.right->ToString() + "\n";
	}
	return extra_info;
}
