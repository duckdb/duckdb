#include "duckdb/optimizer/join_order/join_node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

JoinNode::JoinNode(JoinRelationSet &set)
    : set(set), info(nullptr), left(nullptr), right(nullptr) {}


JoinNode::JoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, optional_ptr<JoinNode> left, optional_ptr<JoinNode> right)
    : set(set), info(info), left(left), right(right) {

}

unique_ptr<EstimatedProperties> EstimatedProperties::Copy() {
	auto result = make_uniq<EstimatedProperties>(cardinality, cost);
	return result;
}


string JoinNode::ToString() {
	string result = "-------------------------------\n";
	result += set.ToString() + "\n";
//	result += "card = " + to_string(GetCardinality<double>()) + "\n";
	bool is_cartesian = false;
//	if (left && right) {
//		is_cartesian = (GetCardinality<double>() == left->GetCardinality<double>() * right->GetCardinality<double>());
//	}
//	result += "cartesian = " + to_string(is_cartesian) + "\n";
//	result += "cost = " + to_string(estimated_props->GetCost<double>()) + "\n";
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
