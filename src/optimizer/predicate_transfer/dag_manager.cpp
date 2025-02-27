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
	ExtractEdges(plan, joins);
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
	if (!reverse) {
		auto in = use_bf->GetColApplied()[0].table_index;
		nodes[in]->AddIn(create_table, use_bf, true);
	} else {
		auto out = use_bf->GetColApplied()[0].table_index;
		nodes[out]->AddIn(create_table, use_bf, false);
	}
}

// extract the edges of the hypergraph, creating a list of filters and their associated bindings.
void DAGManager::ExtractEdges(LogicalOperator &op, vector<reference<LogicalOperator>> &join_operators) {
	auto &sorted_nodes = nodes_manager.getSortedNodes();
	expression_set_t filter_set;
	for (auto &join_op : join_operators) {
		auto &j_op = join_op.get();
		if (j_op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    j_op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			auto &join = j_op.Cast<LogicalComparisonJoin>();
			D_ASSERT(join.expressions.empty());
			for (auto &cond : join.conditions) {
				if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
					continue;
				}
				auto comparison =
				    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
				if (filter_set.find(*comparison) == filter_set.end()) {
					filter_set.insert(*comparison);
					ColumnBinding left_binding;
					if (comparison->left->type == ExpressionType::BOUND_COLUMN_REF) {
						auto &colref = comparison->left->Cast<BoundColumnRefExpression>();
						left_binding = colref.binding;
					}
					ColumnBinding right_binding;
					if (comparison->right->type == ExpressionType::BOUND_COLUMN_REF) {
						auto &colref = comparison->right->Cast<BoundColumnRefExpression>();
						right_binding = colref.binding;
					}
					idx_t left_table = nodes_manager.FindRename(left_binding).table_index;
					idx_t right_table = nodes_manager.FindRename(right_binding).table_index;
					auto left_node = nodes_manager.GetNode(left_table);
					if (left_node == nullptr) {
						continue;
					}
					auto right_node = nodes_manager.GetNode(right_table);
					if (right_node == nullptr) {
						continue;
					}
					idx_t left_node_in_order = 0;
					for (idx_t i = 0; i < sorted_nodes.size(); i++) {
						if (sorted_nodes[i] == left_node) {
							left_node_in_order = i;
							break;
						}
					}
					idx_t right_node_in_order = 0;
					for (idx_t i = 0; i < sorted_nodes.size(); i++) {
						if (sorted_nodes[i] == right_node) {
							right_node_in_order = i;
							break;
						}
					}
					if (join.join_type == JoinType::INNER || join.join_type == JoinType::SEMI ||
					    join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::MARK) {
						if (left_node_in_order > right_node_in_order) {
							auto filter_info =
							    make_shared_ptr<DAGEdgeInfo>(std::move(comparison), *left_node, *right_node);
							auto key1 = make_pair(right_table, left_table);
							auto key2 = make_pair(left_table, right_table);
							if (edges.find(key1) == edges.end()) {
								edges[key1] = vector<shared_ptr<DAGEdgeInfo>>();
								edges[key2] = vector<shared_ptr<DAGEdgeInfo>>();
							}
							edges[key1].emplace_back(filter_info);
							edges[key2].emplace_back(filter_info);
						} else {
							auto filter_info =
							    make_shared_ptr<DAGEdgeInfo>(std::move(comparison), *right_node, *left_node);
							auto key1 = make_pair(right_table, left_table);
							auto key2 = make_pair(left_table, right_table);
							if (edges.find(key1) == edges.end()) {
								edges[key1] = vector<shared_ptr<DAGEdgeInfo>>();
								edges[key2] = vector<shared_ptr<DAGEdgeInfo>>();
							}
							edges[key1].emplace_back(filter_info);
							edges[key2].emplace_back(filter_info);
						}
					} else if (join.join_type == JoinType::LEFT) {
						if (left_node_in_order > right_node_in_order) {
							auto filter_info =
							    make_shared_ptr<DAGEdgeInfo>(std::move(comparison), *left_node, *right_node);
							filter_info->large_protect = true;
							auto key1 = make_pair(right_table, left_table);
							auto key2 = make_pair(left_table, right_table);
							if (edges.find(key1) == edges.end()) {
								edges[key1] = vector<shared_ptr<DAGEdgeInfo>>();
								edges[key2] = vector<shared_ptr<DAGEdgeInfo>>();
							}
							edges[key1].emplace_back(filter_info);
							edges[key2].emplace_back(filter_info);
						} else {
							auto filter_info =
							    make_shared_ptr<DAGEdgeInfo>(std::move(comparison), *right_node, *left_node);
							filter_info->small_protect = true;
							auto key1 = make_pair(right_table, left_table);
							auto key2 = make_pair(left_table, right_table);
							if (edges.find(key1) == edges.end()) {
								edges[key1] = vector<shared_ptr<DAGEdgeInfo>>();
								edges[key2] = vector<shared_ptr<DAGEdgeInfo>>();
							}
							edges[key1].emplace_back(filter_info);
							edges[key2].emplace_back(filter_info);
						}
					} else if (join.join_type == JoinType::RIGHT) {
						if (left_node_in_order > right_node_in_order) {
							auto filter_info =
							    make_shared_ptr<DAGEdgeInfo>(std::move(comparison), *left_node, *right_node);
							filter_info->small_protect = true;
							auto key1 = make_pair(right_table, left_table);
							auto key2 = make_pair(left_table, right_table);
							if (edges.find(key1) == edges.end()) {
								edges[key1] = vector<shared_ptr<DAGEdgeInfo>>();
								edges[key2] = vector<shared_ptr<DAGEdgeInfo>>();
							}
							edges[key1].emplace_back(filter_info);
							edges[key2].emplace_back(filter_info);
						} else {
							auto filter_info =
							    make_shared_ptr<DAGEdgeInfo>(std::move(comparison), *right_node, *left_node);
							filter_info->large_protect = true;
							auto key1 = make_pair(right_table, left_table);
							auto key2 = make_pair(left_table, right_table);
							if (edges.find(key1) == edges.end()) {
								edges[key1] = vector<shared_ptr<DAGEdgeInfo>>();
								edges[key2] = vector<shared_ptr<DAGEdgeInfo>>();
							}
							edges[key1].emplace_back(filter_info);
							edges[key2].emplace_back(filter_info);
						}
					}
				}
			}
		}
	}
	return;
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

	for (auto &filter_and_binding : selected_edges) {
		if (filter_and_binding) {
			idx_t large = NodesManager::GetScalarTableIndex(&filter_and_binding->large_);
			idx_t small = NodesManager::GetScalarTableIndex(&filter_and_binding->small_);

			D_ASSERT(large != -1 && small != -1);

			auto small_node = nodes[small].get();
			auto large_node = nodes[large].get();
			// smaller one has higher priority
			if (small_node->priority > large_node->priority) {
				if (!filter_and_binding->large_protect && !filter_and_binding->small_protect) {
					small_node->AddIn(large_node->id, filter_and_binding->filter.get(), true);
					small_node->AddOut(large_node->id, filter_and_binding->filter.get(), false);
					large_node->AddOut(small_node->id, filter_and_binding->filter.get(), true);
					large_node->AddIn(small_node->id, filter_and_binding->filter.get(), false);
				} else if (filter_and_binding->large_protect && !filter_and_binding->small_protect) {
					small_node->AddIn(large_node->id, filter_and_binding->filter.get(), true);
					large_node->AddOut(small_node->id, filter_and_binding->filter.get(), true);
				} else if (!filter_and_binding->large_protect && filter_and_binding->small_protect) {
					small_node->AddOut(large_node->id, filter_and_binding->filter.get(), false);
					large_node->AddIn(small_node->id, filter_and_binding->filter.get(), false);
				}
			} else {
				if (!filter_and_binding->large_protect && !filter_and_binding->small_protect) {
					small_node->AddOut(large_node->id, filter_and_binding->filter.get(), true);
					small_node->AddIn(large_node->id, filter_and_binding->filter.get(), false);
					large_node->AddIn(small_node->id, filter_and_binding->filter.get(), true);
					large_node->AddOut(small_node->id, filter_and_binding->filter.get(), false);
				} else if (filter_and_binding->large_protect && !filter_and_binding->small_protect) {
					small_node->AddIn(large_node->id, filter_and_binding->filter.get(), false);
					large_node->AddOut(small_node->id, filter_and_binding->filter.get(), false);
				} else if (!filter_and_binding->large_protect && filter_and_binding->small_protect) {
					small_node->AddOut(large_node->id, filter_and_binding->filter.get(), true);
					large_node->AddIn(small_node->id, filter_and_binding->filter.get(), true);
				}
			}
		}
	}
}

vector<GraphNode *> DAGManager::GetNeighbors(idx_t node_id) {
	vector<GraphNode *> result;
	for (auto &filter_and_binding : edges) {
		for (auto &edge : filter_and_binding.second) {
			if (&edge->large_ == nodes_manager.GetNode(node_id)) {
				auto &op = edge->small_;
				int64_t another_node_id = NodesManager::GetScalarTableIndex(&op);
				D_ASSERT(another_node_id != -1);
				result.emplace_back(nodes[another_node_id].get());
			} else if (&edge->small_ == nodes_manager.GetNode(node_id)) {
				auto &op = edge->large_;
				int64_t another_node_id = NodesManager::GetScalarTableIndex(&op);
				D_ASSERT(another_node_id != -1);
				result.emplace_back(nodes[another_node_id].get());
			}
		}
	}
	return result;
}
} // namespace duckdb