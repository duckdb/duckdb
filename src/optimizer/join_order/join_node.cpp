#include "duckdb/optimizer/join_order/join_node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

JoinNode::JoinNode(JoinRelationSet &set) : set(set) {
}

JoinNode::JoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, JoinNode &left, JoinNode &right, double cost)
    : set(set), info(info), left(&left), right(&right), cost(cost) {
}

unique_ptr<EstimatedProperties> EstimatedProperties::Copy() {
	auto result = make_uniq<EstimatedProperties>(cardinality, cost);
	return result;
}

string JoinNode::ToString() {
	string result = "-------------------------------\n";
	result += set.ToString() + "\n";
	result += "cost = " + to_string(cost) + "\n";
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

void JoinNode::Print() {
	Printer::Print(ToString());
}

void JoinNode::Verify() {
#ifdef DEBUG
	D_ASSERT(set.count >= 1);
	idx_t left_count = 0, right_count = 0;
	if (left) {
		left->Verify();
		left_count = left->set.count;
	}
	if (right) {
		right->Verify();
		right_count = right->set.count;
	}
	D_ASSERT(set.count == left_count + right_count || set.count == 1);
#endif
}

} // namespace duckdb
