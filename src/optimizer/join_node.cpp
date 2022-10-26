#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/optimizer/join_node.hpp"

namespace duckdb {

JoinNode::JoinNode(JoinRelationSet *set, const double base_cardinality)
    : set(set), info(nullptr), has_filter(false), left(nullptr), right(nullptr), base_cardinality(base_cardinality) {
	estimated_props = make_unique<EstimatedProperties>(base_cardinality, 0);
}

JoinNode::JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right,
                   const double base_cardinality, double cost)
    : set(set), info(info), has_filter(false), left(left), right(right), base_cardinality(base_cardinality) {
	estimated_props = make_unique<EstimatedProperties>(base_cardinality, cost);
}

unique_ptr<EstimatedProperties> EstimatedProperties::Copy() {
	auto result = make_unique<EstimatedProperties>(cardinality, cost);
	return result;
}

double JoinNode::GetCost() {
	return estimated_props->GetCost();
}

void JoinNode::SetCost(double cost) {
	estimated_props->SetCost(cost);
}

double JoinNode::GetBaseTableCardinality() {
	if (set->count > 1) {
		throw InvalidInputException("Cannot call get base table cardinality on intermediate join node");
	}
	return base_cardinality;
}

void JoinNode::SetBaseTableCardinality(double base_card) {
	base_cardinality = base_card;
}

void JoinNode::SetEstimatedCardinality(double estimated_card) {
	estimated_props->SetCardinality(estimated_card);
}

string JoinNode::ToString() {
	if (!set) {
		return "";
	}
	string result = "-------------------------------\n";
	result += set->ToString() + "\n";
	result += "card = " + to_string(GetCardinality<double>()) + "\n";
	bool is_cartesian = false;
	if (left && right) {
		is_cartesian = (GetCardinality<double>() == left->GetCardinality<double>() * right->GetCardinality<double>());
	}
	result += "cartesian = " + to_string(is_cartesian) + "\n";
	result += "cost = " + to_string(estimated_props->GetCost()) + "\n";
	result += "left = \n";
	if (left) {
		result += left->ToString();
	}
	result += "right = \n";
	if (right) {
		result += right->ToString();
	}
	return result;
}
} // namespace duckdb
