#include "duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include <queue>

namespace duckdb {

/* Build DAG according to the query plan */
bool TransferGraphManager::Build(LogicalOperator &plan) {
	// Extract all operators, including table operators and join operators
	vector<reference<LogicalOperator>> joins = table_operator_manager.ExtractOperators(plan);
	if (table_operator_manager.table_operators.size() < 2) {
		return false;
	}

	// Getting graph edges information from join operators
	ExtractEdgesInfo(joins);
	if (edges_info.empty()) {
		return false;
	}

	CreatePredicateTransferGraph();
	return true;
}

void TransferGraphManager::AddBF(idx_t create_table, const shared_ptr<BlockedBloomFilter> &use_bf, bool reverse) {
	bool is_forward = !reverse;
	auto node_idx = use_bf->GetColApplied()[0].table_index;
	transfer_graph[node_idx]->Add(create_table, use_bf, is_forward, true);
}

void TransferGraphManager::ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators) {
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
			idx_t left_table = table_operator_manager.FindRename(left_binding).table_index;
			idx_t right_table = table_operator_manager.FindRename(right_binding).table_index;
			auto left_node = table_operator_manager.GetTableOperator(left_table);
			auto right_node = table_operator_manager.GetTableOperator(right_table);
			if (!left_node || !right_node)
				continue;

			// Determine the order of the nodes.
			auto left_order = table_operator_manager.GetTableOperatorOrder(left_node);
			auto right_order = table_operator_manager.GetTableOperatorOrder(right_node);
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
			if (edges_info.find(key1) == edges_info.end()) {
				edges_info[key1] = vector<shared_ptr<EdgeInfo>>();
				edges_info[key2] = vector<shared_ptr<EdgeInfo>>();
			}
			edges_info[key1].emplace_back(edge);
			edges_info[key2].emplace_back(edge);
		}
	}
}

pair<int, int> TransferGraphManager::FindEdge(const unordered_set<int> &constructed_set,
                                              const unordered_set<int> &unconstructed_set) {
	idx_t max_weight = 0, max_card = 0;
	pair<int, int> result {-1, -1};

	for (auto i : unconstructed_set) {
		for (auto j : constructed_set) {
			auto key = make_pair(j, i);
			auto it = edges_info.find(key);
			if (it == edges_info.end())
				continue;

			idx_t card = table_operator_manager.GetTableOperator(i)->estimated_cardinality;
			idx_t weight = it->second.size();

			if (weight > max_weight || (weight == max_weight && card > max_card)) {
				max_weight = weight;
				max_card = card;
				result = key;
			}
		}
	}
	return result;
}

void TransferGraphManager::LargestRoot(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<int> constructed_set;
	unordered_set<int> unconstructed_set;
	int prior_flag = table_operator_manager.table_operators.size() - 1;
	int root = -1;

	// Create Vertices
	for (auto &vertex : table_operator_manager.table_operators) {
		// Set the last operator as root
		if (vertex.second == sorted_nodes.back()) {
			auto node = make_uniq<GraphNode>(vertex.first, vertex.second->estimated_cardinality, true);
			node->priority = prior_flag--;
			constructed_set.emplace(vertex.first);
			transfer_graph[vertex.first] = std::move(node);
			root = vertex.first;
		} else {
			auto node = make_uniq<GraphNode>(vertex.first, vertex.second->estimated_cardinality, false);
			unconstructed_set.emplace(vertex.first);
			transfer_graph[vertex.first] = std::move(node);
		}
	}

	// delete root
	transfer_order.emplace_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);
	while (!unconstructed_set.empty()) {
		// Old node at first, new add node at second
		auto selected_edge = FindEdge(constructed_set, unconstructed_set);
		if (selected_edge.first == -1 && selected_edge.second == -1) {
			break;
		}
		if (edges_info.find(selected_edge) != edges_info.end()) {
			for (auto &v : edges_info[selected_edge]) {
				selected_edges.emplace_back(std::move(v));
			}
		}
		auto node = transfer_graph[selected_edge.second].get();
		node->priority = prior_flag--;
		transfer_order.emplace_back(table_operator_manager.GetTableOperator(node->id));
		table_operator_manager.table_operators.erase(node->id);
		unconstructed_set.erase(selected_edge.second);
		constructed_set.emplace(selected_edge.second);
	}
}

void TransferGraphManager::CreatePredicateTransferGraph() {
	auto saved_nodes = table_operator_manager.table_operators;
	while (table_operator_manager.table_operators.size() > 0) {
		LargestRoot(table_operator_manager.sorted_table_operators);
		table_operator_manager.SortTableOperators();
	}
	table_operator_manager.table_operators = saved_nodes;

	for (auto &edge : selected_edges) {
		if (!edge)
			continue;

		idx_t large = TableOperatorManager::GetScalarTableIndex(&edge->bigger_table);
		idx_t small = TableOperatorManager::GetScalarTableIndex(&edge->smaller_table);

		D_ASSERT(large != -1 && small != -1);

		auto small_node = transfer_graph[small].get();
		auto large_node = transfer_graph[large].get();

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