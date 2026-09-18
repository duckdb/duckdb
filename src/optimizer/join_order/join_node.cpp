#include "duckdb/optimizer/join_order/join_node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

DPJoinNode::DPJoinNode(JoinRelationSet &set)
    : set(set), join_operator(nullptr), generated_cross_product(false), is_leaf(true), left_set(set), right_set(set),
      cost(0) {
}

DPJoinNode::DPJoinNode(JoinRelationSet &set, optional_ptr<JoinOrderOperator> join_operator,
                       vector<reference<JoinPredicate>> predicates, bool generated_cross_product, JoinRelationSet &left,
                       JoinRelationSet &right, double cost)
    : set(set), join_operator(join_operator), predicates(std::move(predicates)),
      generated_cross_product(generated_cross_product), is_leaf(false), left_set(left), right_set(right), cost(cost) {
}

} // namespace duckdb
