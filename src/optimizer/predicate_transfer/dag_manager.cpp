#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include <queue>

namespace duckdb {

/* Build DAG according to the query plan */
bool DAGManager::Build(LogicalOperator &plan) {
	// Extract all nodes (and joins)
	vector<reference<LogicalOperator>> joins = binding_manager.ExtractNodesAndJoins(plan);
	if (binding_manager.nodes.size() < 2) {
		return false;
	}

	// Transform joins into edges
	ExtractEdges(joins);
	if (edges.empty()) {
		return false;
	}

	// Create the query_graph hyper edges
	CreateDAG();
	return true;
}

vector<LogicalOperator *> &DAGManager::GetExecutionOrder() {
	// The root as first
	return TransferOrder;
}

void DAGManager::AddBF(idx_t create_table, const shared_ptr<BlockedBloomFilter> &use_bf, bool reverse) {
	bool is_forward = !reverse;
	auto node_idx = use_bf->GetColApplied()[0].table_index;
	nodes[node_idx]->Add(create_table, use_bf, is_forward, true);
}

// extract the edges of the hypergraph, creating a list of filters and their associated bindings.
void DAGManager::ExtractEdges(vector<reference<LogicalOperator>> &join_operators) {
	expression_set_t conditions;

	for (auto &join_op : join_operators) {
		auto &j_op = join_op.get();

		if (j_op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    j_op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			continue;
		}

		auto &join = j_op.Cast<LogicalComparisonJoin>();
		D_ASSERT(join.expressions.empty());

		for (auto &cond : join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL)
				continue;

			auto comparison =
			    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
			if (conditions.find(*comparison) != conditions.end())
				continue;
			conditions.insert(*comparison);

			// Extract column bindings from the left and right expressions.
			ColumnBinding left_binding, right_binding;
			if (comparison->left->type == ExpressionType::BOUND_COLUMN_REF) {
				left_binding = comparison->left->Cast<BoundColumnRefExpression>().binding;
			}
			if (comparison->right->type == ExpressionType::BOUND_COLUMN_REF) {
				right_binding = comparison->right->Cast<BoundColumnRefExpression>().binding;
			}

			// Determine table indices and corresponding nodes.
			idx_t left_table = binding_manager.FindRename(left_binding).table_index;
			idx_t right_table = binding_manager.FindRename(right_binding).table_index;
			auto left_node = binding_manager.GetNode(left_table);
			auto right_node = binding_manager.GetNode(right_table);
			if (!left_node || !right_node)
				continue;

			// Determine the order of the nodes.
			auto left_order = binding_manager.GetNodeOrder(left_node);
			auto right_order = binding_manager.GetNodeOrder(right_node);
			bool swap_order = left_order > right_order;

			// Use swap_order to determine effective ordering.
			auto effective_left = (swap_order ? left_node : right_node);
			auto effective_right = (swap_order ? right_node : left_node);

			// Create the edge using the effective node order.
			shared_ptr<EdgeInfo> edge =
			    make_shared_ptr<EdgeInfo>(std::move(comparison), *effective_left, *effective_right);

			// Set protection flags based on join type.
			switch (join.join_type) {
			case JoinType::INNER:
			case JoinType::SEMI:
			case JoinType::RIGHT_SEMI:
			case JoinType::MARK:
				// No protection flags needed.
				break;
			case JoinType::LEFT:
				if (swap_order)
					edge->protect_bigger_side = true;
				else
					edge->protect_smaller_side = true;
				break;
			case JoinType::RIGHT:
				if (swap_order)
					edge->protect_smaller_side = true;
				else
					edge->protect_bigger_side = true;
				break;
			default:
				// Unsupported join type; skip.
				continue;
			}

			// Insert the edge info into the map for both key orders.
			auto key1 = make_pair(right_table, left_table);
			auto key2 = make_pair(left_table, right_table);
			if (edges.find(key1) == edges.end()) {
				edges[key1] = vector<shared_ptr<EdgeInfo>>();
				edges[key2] = vector<shared_ptr<EdgeInfo>>();
			}
			edges[key1].emplace_back(edge);
			edges[key2].emplace_back(edge);
		}
	}
}

pair<int, int> DAGManager::FindEdge(unordered_set<int> &constructed_set, unordered_set<int> &unconstructed_set) {
	idx_t max_weight = 0;
	idx_t max_card = 0;
	auto result = make_pair(-1, -1);
	for (int i : unconstructed_set) {
		for (int j : constructed_set) {
			auto key = make_pair(j, i);
			if (edges.find(key) != edges.end()) {
				auto card = binding_manager.GetNode(i)->estimated_cardinality;
				auto weight = edges[key].size();
				if (weight > max_weight) {
					max_weight = weight;
					max_card = card;
					result = key;
				} else if (weight == max_weight) {
					if (card > max_card) {
						max_card = card;
						result = key;
					}
				}
			}
		}
	}
	return result;
}

void DAGManager::LargestRoot(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<int> constructed_set;
	unordered_set<int> unconstructed_set;
	int prior_flag = binding_manager.nodes.size() - 1;
	int root = -1;

	// Create Vertices
	for (auto &vertex : binding_manager.nodes) {
		// Set the last operator as root
		if (vertex.second == sorted_nodes.back()) {
			auto node = make_uniq<GraphNode>(vertex.first, vertex.second->estimated_cardinality, true);
			node->priority = prior_flag--;
			constructed_set.emplace(vertex.first);
			nodes[vertex.first] = std::move(node);
			root = vertex.first;
		} else {
			auto node = make_uniq<GraphNode>(vertex.first, vertex.second->estimated_cardinality, false);
			unconstructed_set.emplace(vertex.first);
			nodes[vertex.first] = std::move(node);
		}
	}

	// delete root
	TransferOrder.emplace_back(binding_manager.GetNode(root));
	binding_manager.nodes.erase(root);
	while (!unconstructed_set.empty()) {
		// Old node at first, new add node at second
		auto selected_edge = FindEdge(constructed_set, unconstructed_set);
		if (selected_edge.first == -1 && selected_edge.second == -1) {
			break;
		}
		if (edges.find(selected_edge) != edges.end()) {
			for (auto &v : edges[selected_edge]) {
				selected_edges.emplace_back(std::move(v));
			}
		}
		auto node = nodes[selected_edge.second].get();
		node->priority = prior_flag--;
		TransferOrder.emplace_back(binding_manager.GetNode(node->id));
		binding_manager.nodes.erase(node->id);
		unconstructed_set.erase(selected_edge.second);
		constructed_set.emplace(selected_edge.second);
	}
}

void DAGManager::CreateDAG() {
	auto saved_nodes = binding_manager.nodes;
	while (binding_manager.nodes.size() > 0) {
		LargestRoot(binding_manager.sorted_nodes);
		binding_manager.SortNodes();
	}
	binding_manager.nodes = saved_nodes;

	for (auto &edge : selected_edges) {
		if (!edge)
			continue;

		idx_t large = NodeBindingManager::GetScalarTableIndex(&edge->bigger_table);
		idx_t small = NodeBindingManager::GetScalarTableIndex(&edge->smaller_table);

		D_ASSERT(large != -1 && small != -1);

		auto small_node = nodes[small].get();
		auto large_node = nodes[large].get();

		// Determine the ordering based on priority:
		// If small_node's priority is higher than large_node's, use one set of flags,
		// otherwise use the opposite.
		bool forward = (small_node->priority > large_node->priority);

		if (!edge->protect_bigger_side && !edge->protect_smaller_side) {
			// No protection
			small_node->Add(large_node->id, edge->condition.get(), forward, true);
			large_node->Add(small_node->id, edge->condition.get(), forward, false);

			small_node->Add(large_node->id, edge->condition.get(), !forward, false);
			large_node->Add(small_node->id, edge->condition.get(), !forward, true);
		} else if (edge->protect_bigger_side && !edge->protect_smaller_side) {
			// When only large is protected,
			small_node->Add(large_node->id, edge->condition.get(), forward, true);
			large_node->Add(small_node->id, edge->condition.get(), !forward, false);
		} else if (!edge->protect_bigger_side && edge->protect_smaller_side) {
			// When only small is protected,
			small_node->Add(large_node->id, edge->condition.get(), forward, false);
			large_node->Add(small_node->id, edge->condition.get(), !forward, true);
		}
	}
}
} // namespace duckdb