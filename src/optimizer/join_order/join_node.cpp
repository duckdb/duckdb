#include "duckdb/optimizer/join_order/join_node.hpp"

namespace duckdb {
struct JoinRelationSet;

DPJoinNode::DPJoinNode(JoinRelationSet &set) : set(set), info(nullptr), is_leaf(true), left_set(set), right_set(set) {
}

DPJoinNode::DPJoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, JoinRelationSet &left,
                       JoinRelationSet &right, double cost)
    : set(set), info(info), is_leaf(false), left_set(left), right_set(right), cost(cost) {
}

} // namespace duckdb
