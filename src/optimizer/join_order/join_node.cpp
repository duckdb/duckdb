#include "duckdb/optimizer/join_order/join_node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {
//
// JoinNode::JoinNode(JoinRelationSet &set) : set(set) {
//}
//
// JoinNode::JoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, unique_ptr<JoinNode> left,
//                   unique_ptr<JoinNode> right, double cost)
//    : set(set), info(info), left(std::move(left)), right(std::move(right)), cost(cost) {
//}
//
// unique_ptr<EstimatedProperties> EstimatedProperties::Copy() {
//	auto result = make_uniq<EstimatedProperties>(cardinality, cost);
//	return result;
//}
//
// string JoinNode::ToString() {
//	string result = "-------------------------------\n";
//	result += set.ToString() + "\n";
//	result += "cost = " + to_string(cost) + "\n";
//	result += "left = \n";
//	if (left) {
//		result += left->ToString();
//	}
//	result += "right = \n";
//	if (right) {
//		result += right->ToString();
//	}
//	return result;
//}
//
// void JoinNode::Print() {
//	Printer::Print(ToString());
//}

DPJoinNode::DPJoinNode(JoinRelationSet &set) : set(set), info(nullptr), is_leaf(true), left_set(set), right_set(set) {
}

DPJoinNode::DPJoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, JoinRelationSet &left,
                       JoinRelationSet &right, double cost)
    : set(set), info(info), is_leaf(false), left_set(left), right_set(right), cost(cost) {
}

} // namespace duckdb
