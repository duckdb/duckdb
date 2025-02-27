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
	nodes_manager.Reset();

	// Extract all the vertex nodes (and joins)
	vector<reference<LogicalOperator>> joins;
	nodes_manager.ExtractNodes(plan, joins);
	nodes_manager.DuplicateNodes();
	if (nodes_manager.NumNodes() < 2) {
		return false;
	}
	nodes_manager.SortNodes();

	// extract the edges of the hypergraph, creating a list of filters and their associated bindings.
	ExtractEdges(joins);
	if (edges.size() == 0) {
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

void DAGManager::Add(idx_t create_table, shared_ptr<BlockedBloomFilter> use_bf, bool reverse) {
	bool is_forward = !reverse;
	auto node_idx = use_bf->GetColApplied()[0].table_index;
	nodes[node_idx]->AddIn(create_table, use_bf, is_forward);
}

// extract the edges of the hypergraph, creating a list of filters and their associated bindings.
void DAGManager::ExtractEdges(vector<reference<LogicalOperator>> &join_operators) {
	// Track processed comparisons to avoid duplicates.
	expression_set_t conditions;

	for (auto &join_op : join_operators) {
		auto &j_op = join_op.get();

		// Only process comparison or delim joins.
		if (j_op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    j_op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			continue;
		}

		auto &join = j_op.Cast<LogicalComparisonJoin>();
		D_ASSERT(join.expressions.empty());

		for (auto &cond : join.conditions) {
			// Only consider equality comparisons.
			if (cond.comparison != ExpressionType::COMPARE_EQUAL)
				continue;

			// Create a unique comparison expression.
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
			idx_t left_table = nodes_manager.FindRename(left_binding).table_index;
			idx_t right_table = nodes_manager.FindRename(right_binding).table_index;
			auto left_node = nodes_manager.GetNode(left_table);
			auto right_node = nodes_manager.GetNode(right_table);
			if (!left_node || !right_node)
				continue;

			// Determine the order of the nodes.
			auto left_order = nodes_manager.GetNodeOrder(left_node);
			auto right_order = nodes_manager.GetNodeOrder(right_node);
			bool swap_order = left_order > right_order;

			// Use swap_order to determine effective ordering.
			auto effective_left = (swap_order ? left_node : right_node);
			auto effective_right = (swap_order ? right_node : left_node);

			// Create the edge using the effective node order.
			shared_ptr<DAGEdgeInfo> edge =
			    make_shared_ptr<DAGEdgeInfo>(std::move(comparison), *effective_left, *effective_right);

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
					edge->large_protect = true;
				else
					edge->small_protect = true;
				break;
			case JoinType::RIGHT:
				if (swap_order)
					edge->small_protect = true;
				else
					edge->large_protect = true;
				break;
			default:
				// Unsupported join type; skip.
				continue;
			}

			// Insert the edge info into the map for both key orders.
			auto key1 = make_pair(right_table, left_table);
			auto key2 = make_pair(left_table, right_table);
			if (edges.find(key1) == edges.end()) {
				edges[key1] = vector<shared_ptr<DAGEdgeInfo>>();
				edges[key2] = vector<shared_ptr<DAGEdgeInfo>>();
			}
			edges[key1].emplace_back(edge);
			edges[key2].emplace_back(edge);
		}
	}
}

struct DAGNodeCompare {
	bool operator()(const GraphNode *lhs, const GraphNode *rhs) const {
		return lhs->est_cardinality < rhs->est_cardinality;
	}
};

pair<int, int> DAGManager::FindEdge(unordered_set<int> &constructed_set, unordered_set<int> &unconstructed_set) {
	idx_t max_weight = 0;
	idx_t max_card = 0;
	auto result = make_pair(-1, -1);
	for (int i : unconstructed_set) {
		for (int j : constructed_set) {
			auto key = make_pair(j, i);
			if (edges.find(key) != edges.end()) {
				auto card = nodes_manager.GetNode(i)->estimated_cardinality;
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
	int prior_flag = nodes_manager.NumNodes() - 1;
	int root = -1;

	// Create Vertices
	for (auto &vertex : nodes_manager.GetNodes()) {
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
	TransferOrder.emplace_back(nodes_manager.GetNode(root));
	nodes_manager.EraseNode(root);
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
		TransferOrder.emplace_back(nodes_manager.GetNode(node->id));
		nodes_manager.EraseNode(node->id);
		unconstructed_set.erase(selected_edge.second);
		constructed_set.emplace(selected_edge.second);
	}
}

void DAGManager::CreateDAG() {
	while (nodes_manager.GetNodes().size() > 0) {
		auto &sorted_nodes = nodes_manager.getSortedNodes();
		LargestRoot(sorted_nodes);
		nodes_manager.SortNodes();
	}
	nodes_manager.RecoverNodes();

	for (auto &edge : selected_edges) {
		if (!edge)
			continue;

		idx_t large = NodesManager::GetScalarTableIndex(&edge->large_);
		idx_t small = NodesManager::GetScalarTableIndex(&edge->small_);

		D_ASSERT(large != -1 && small != -1);

		auto small_node = nodes[small].get();
		auto large_node = nodes[large].get();

		// Determine the ordering based on priority:
		// If small_node's priority is higher than large_node's, use one set of flags,
		// otherwise use the opposite.
		bool high_priority_first = (small_node->priority > large_node->priority);
		bool in_flag = high_priority_first ? true : false;
		bool out_flag = high_priority_first ? false : true;

		if (!edge->large_protect && !edge->small_protect) {
			// No protection: set the in/out flags based on priority.
			small_node->AddIn(large_node->id, edge->filter.get(), in_flag);
			small_node->AddOut(large_node->id, edge->filter.get(), out_flag);
			large_node->AddOut(small_node->id, edge->filter.get(), in_flag);
			large_node->AddIn(small_node->id, edge->filter.get(), out_flag);
		} else if (edge->large_protect && !edge->small_protect) {
			// When only large is protected, force a true on the corresponding edge.
			small_node->AddIn(large_node->id, edge->filter.get(), in_flag);
			large_node->AddOut(small_node->id, edge->filter.get(), true);
		} else if (!edge->large_protect && edge->small_protect) {
			// When only small is protected, force the out flag for small.
			small_node->AddOut(large_node->id, edge->filter.get(), out_flag);
			large_node->AddIn(small_node->id, edge->filter.get(), out_flag);
		}
	}
}

vector<GraphNode *> DAGManager::GetNeighbors(idx_t node_id) {
	vector<GraphNode *> result;
	for (auto &edge : edges) {
		for (auto &info : edge.second) {
			if (&info->large_ == nodes_manager.GetNode(node_id)) {
				auto &op = info->small_;
				int64_t another_node_id = NodesManager::GetScalarTableIndex(&op);
				D_ASSERT(another_node_id != -1);
				result.emplace_back(nodes[another_node_id].get());
			} else if (&info->small_ == nodes_manager.GetNode(node_id)) {
				auto &op = info->large_;
				int64_t another_node_id = NodesManager::GetScalarTableIndex(&op);
				D_ASSERT(another_node_id != -1);
				result.emplace_back(nodes[another_node_id].get());
			}
		}
	}
	return result;
}
} // namespace duckdb