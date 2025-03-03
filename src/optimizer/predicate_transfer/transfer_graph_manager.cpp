#include "duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

#include <queue>

namespace duckdb {

bool TransferGraphManager::Build(LogicalOperator &plan) {
	// 1. Extract all operators, including table operators and join operators
	const vector<reference<LogicalOperator>> joins = table_operator_manager.ExtractOperators(plan);
	if (table_operator_manager.table_operators.size() < 2) {
		return false;
	}

	// 2. Getting graph edges information from join operators
	ExtractEdgesInfo(joins);
	if (edges_info.empty()) {
		return false;
	}

	// 3. Create the transfer graph
	CreatePredicateTransferGraph();
	return true;
}

void TransferGraphManager::AddFilterPlan(idx_t create_table, const shared_ptr<BloomFilterPlan> &filter_plan,
                                         bool reverse) {
	bool is_forward = !reverse;
	auto node_idx = filter_plan->apply[0].table_index;
	transfer_graph[node_idx]->Add(create_table, filter_plan, is_forward, true);
}

void TransferGraphManager::ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators) {
	expression_set_t conditions;

	for (size_t i = 0; i < join_operators.size(); i++) {
		auto &join = join_operators[i].get();
		if (join.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    join.type != LogicalOperatorType::LOGICAL_DELIM_JOIN)
			continue;

		auto &comp_join = join.Cast<LogicalComparisonJoin>();
		D_ASSERT(comp_join.expressions.empty());

		for (size_t j = 0; j < comp_join.conditions.size(); j++) {
			auto &cond = comp_join.conditions[j];
			if (cond.comparison != ExpressionType::COMPARE_EQUAL)
				continue;

			unique_ptr<BoundComparisonExpression> comparison(
			    new BoundComparisonExpression(cond.comparison, cond.left->Copy(), cond.right->Copy()));
			if (!conditions.insert(*comparison).second)
				continue;

			// Extract column bindings
			ColumnBinding left_binding, right_binding;
			if (comparison->left->type == ExpressionType::BOUND_COLUMN_REF) {
				left_binding = comparison->left->Cast<BoundColumnRefExpression>().binding;
			}
			if (comparison->right->type == ExpressionType::BOUND_COLUMN_REF) {
				right_binding = comparison->right->Cast<BoundColumnRefExpression>().binding;
			}

			// Determine table indices and corresponding nodes
			idx_t left_table = table_operator_manager.GetRenaming(left_binding).table_index;
			idx_t right_table = table_operator_manager.GetRenaming(right_binding).table_index;
			auto left_node = table_operator_manager.GetTableOperator(left_table);
			auto right_node = table_operator_manager.GetTableOperator(right_table);
			if (!left_node || !right_node)
				continue;

			// Order nodes based on priority
			bool swap_order = table_operator_manager.GetTableOperatorOrder(left_node) >
			                  table_operator_manager.GetTableOperatorOrder(right_node);

			LogicalOperator *big_table = swap_order ? left_node : right_node;
			LogicalOperator *small_table = swap_order ? right_node : left_node;

			// Create edge
			shared_ptr<EdgeInfo> edge(new EdgeInfo(std::move(comparison), *big_table, *small_table));

			// Set protection flags
			if (comp_join.join_type == JoinType::LEFT || comp_join.join_type == JoinType::MARK) {
				if (swap_order)
					edge->protect_bigger_side = true;
				else
					edge->protect_smaller_side = true;
			} else if (comp_join.join_type == JoinType::RIGHT) {
				if (swap_order)
					edge->protect_smaller_side = true;
				else
					edge->protect_bigger_side = true;
			} else if (comp_join.join_type != JoinType::INNER && comp_join.join_type != JoinType::SEMI &&
			           comp_join.join_type != JoinType::RIGHT_SEMI && comp_join.join_type != JoinType::MARK) {
				continue; // Unsupported join type
			}

			// Store edge info
			pair<int, int> key1 = make_pair(right_table, left_table);
			pair<int, int> key2 = make_pair(left_table, right_table);
			if (edges_info.find(key1) == edges_info.end()) {
				edges_info[key1] = vector<shared_ptr<EdgeInfo>>();
				edges_info[key2] = vector<shared_ptr<EdgeInfo>>();
			}
			edges_info[key1].push_back(edge);
			edges_info[key2].push_back(edge);
		}
	}
}

void TransferGraphManager::LargestRoot(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<int> constructed_set, unconstructed_set;
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	int root = -1;

	// Create table operators
	for (auto &table_operator : table_operator_manager.table_operators) {
		bool is_root = (table_operator.second == sorted_nodes.back());
		auto node = make_uniq<GraphNode>(table_operator.first, prior_flag--);

		if (is_root) {
			constructed_set.emplace(table_operator.first);
			root = table_operator.first;
		} else {
			unconstructed_set.emplace(table_operator.first);
		}

		transfer_graph.emplace(table_operator.first, std::move(node));
	}

	// Remove root
	transfer_order.emplace_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);

	while (!unconstructed_set.empty()) {
		auto selected_edge = FindEdge(constructed_set, unconstructed_set);
		if (selected_edge.first == -1)
			break;

		if (edges_info.count(selected_edge)) {
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
	while (!table_operator_manager.table_operators.empty()) {
		LargestRoot(table_operator_manager.sorted_table_operators);
		table_operator_manager.SortTableOperators();
	}
	table_operator_manager.table_operators = saved_nodes;

	for (auto &edge : selected_edges) {
		if (!edge) {
			continue;
		}

		idx_t bigger = TableOperatorManager::GetScalarTableIndex(&edge->bigger_table);
		idx_t smaller = TableOperatorManager::GetScalarTableIndex(&edge->smaller_table);

		D_ASSERT(bigger != std::numeric_limits<idx_t>::max() && smaller != std::numeric_limits<idx_t>::max());

		auto small_node = transfer_graph[smaller].get();
		auto large_node = transfer_graph[bigger].get();
		bool swap_order = (small_node->priority > large_node->priority);
		auto *condition = edge->condition.get();

		// forward: from the smaller to the larger
		if (!edge->protect_bigger_side) {
			small_node->Add(large_node->id, condition, !swap_order, false);
			large_node->Add(small_node->id, condition, !swap_order, true);
		}

		// backward: from the larger to the smaller
		if (!edge->protect_smaller_side) {
			small_node->Add(large_node->id, condition, swap_order, true);
			large_node->Add(small_node->id, condition, swap_order, false);
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
			if (it == edges_info.end()) {
				continue;
			}

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

} // namespace duckdb
