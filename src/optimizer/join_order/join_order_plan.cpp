#include "duckdb/optimizer/join_order/join_order_plan.hpp"

namespace duckdb {

JoinOrderPlanMap CopyJoinOrderLeafPlans(const JoinOrderPlanMap &source) {
	JoinOrderPlanMap result;
	for (auto &entry : source) {
		if (entry.second->is_leaf) {
			result.emplace(entry.first, make_uniq<DPJoinNode>(*entry.second));
		}
	}
	return result;
}

CapturedJoinOrderPlan::CapturedJoinOrderPlan(JoinRelationSet &root, JoinOrderPlanMap plans)
    : root(root), plans(std::move(plans)) {
}

unique_ptr<CapturedJoinOrderPlan> CapturedJoinOrderPlan::Capture(const JoinOrderPlanMap &source, JoinRelationSet &root,
                                                                 CostModel &cost_model) {
	vector<reference<JoinRelationSet>> pending {root};
	vector<reference<DPJoinNode>> preorder;
	while (!pending.empty()) {
		// Missing children are a broken closure invariant, not an ordinary failed trial.
		auto &node = *source.at(pending.back());
		pending.pop_back();
		preorder.push_back(node);
		if (!node.is_leaf) {
			pending.push_back(node.left_set);
			pending.push_back(node.right_set);
		}
	}
	JoinOrderPlanMap result;
	for (idx_t i = preorder.size(); i > 0; i--) {
		auto node = make_uniq<DPJoinNode>(preorder[i - 1].get());
		if (!node->is_leaf) {
			auto &left = *result.at(node->left_set);
			auto &right = *result.at(node->right_set);
			node->cost = cost_model.ComputeCost(left, right, node->join_operator, node->set);
			cost_model.UpdateBuildMemory(*node, left, right);
		}
		result.emplace(node->set, std::move(node));
	}
	return unique_ptr<CapturedJoinOrderPlan>(new CapturedJoinOrderPlan(root, std::move(result)));
}

JoinRelationSet &CapturedJoinOrderPlan::GetRoot() const {
	return root;
}

const DPJoinNode &CapturedJoinOrderPlan::GetRootNode() const {
	return *plans.at(root);
}

const JoinOrderPlanMap &CapturedJoinOrderPlan::GetPlans() const {
	return plans;
}

vector<RelationIndex> CapturedJoinOrderPlan::GetLeafOrder() const {
	vector<RelationIndex> order;
	vector<reference<JoinRelationSet>> pending {root};
	while (!pending.empty()) {
		auto &node = *plans.at(pending.back());
		pending.pop_back();
		if (node.is_leaf) {
			D_ASSERT(node.set.count == 1);
			order.push_back(node.set.relations[0]);
		} else {
			pending.push_back(node.right_set);
			pending.push_back(node.left_set);
		}
	}
	return order;
}

JoinOrderPlanMap CapturedJoinOrderPlan::TakePlans() {
	return std::move(plans);
}

} // namespace duckdb
