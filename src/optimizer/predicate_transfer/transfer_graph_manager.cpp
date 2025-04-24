#include "duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

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
	if (neighbor_matrix.empty()) {
		return false;
	}

	// 3. Create the transfer graph
	CreatePredicateTransferGraph();
	return true;
}

void TransferGraphManager::AddFilterPlan(idx_t create_table, const shared_ptr<FilterPlan> &filter_plan, bool reverse) {
	bool is_forward = !reverse;

	D_ASSERT(filter_plan->apply.size() >= 1);
	auto &expr = filter_plan->apply[0]->Cast<BoundColumnRefExpression>().binding;
	auto node_idx = expr.table_index;
	transfer_graph[node_idx]->Add(create_table, filter_plan, is_forward, true);
}

void TransferGraphManager::ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators) {
	unordered_set<hash_t> existed_set;
	auto ComputeConditionHash = [](const JoinCondition &cond) {
		return cond.left->Hash() + cond.right->Hash();
	};

	for (size_t i = 0; i < join_operators.size(); i++) {
		auto &join = join_operators[i].get();
		if (join.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    join.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			continue;
		}

		auto &comp_join = join.Cast<LogicalComparisonJoin>();
		D_ASSERT(comp_join.expressions.empty());

		for (size_t j = 0; j < comp_join.conditions.size(); j++) {
			auto &cond = comp_join.conditions[j];
			if (cond.comparison != ExpressionType::COMPARE_EQUAL ||
			    cond.left->type != ExpressionType::BOUND_COLUMN_REF ||
			    cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}

			if (!cond.left->return_type.IsNumeric() || !cond.right->return_type.IsNumeric()) {
				continue;
			}

			hash_t hash = ComputeConditionHash(cond);
			if (!existed_set.insert(hash).second) {
				continue;
			}

			ColumnBinding left_binding = cond.left->Cast<BoundColumnRefExpression>().binding;
			idx_t left_table = table_operator_manager.GetRenaming(left_binding).table_index;
			auto left_node = table_operator_manager.GetTableOperator(left_table);

			ColumnBinding right_binding = cond.right->Cast<BoundColumnRefExpression>().binding;
			idx_t right_table = table_operator_manager.GetRenaming(right_binding).table_index;
			auto right_node = table_operator_manager.GetTableOperator(right_table);

			if (!left_node || !right_node) {
				continue;
			}

			// Order nodes based on priority
			bool left_is_larger = table_operator_manager.GetTableOperatorOrder(left_node) >
			                      table_operator_manager.GetTableOperatorOrder(right_node);

			LogicalOperator *big_table = left_is_larger ? left_node : right_node;
			LogicalOperator *small_table = left_is_larger ? right_node : left_node;

			// Create edge
			auto comparison =
			    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
			shared_ptr<EdgeInfo> edge(new EdgeInfo(std::move(comparison), *big_table, *small_table));

			// Set protection flags
			switch (comp_join.type) {
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
				if (comp_join.join_type == JoinType::LEFT) {
					(left_is_larger ? edge->protect_bigger_side : edge->protect_smaller_side) = true;
				} else if (comp_join.join_type == JoinType::RIGHT) {
					(left_is_larger ? edge->protect_smaller_side : edge->protect_bigger_side) = true;
				} else if (comp_join.join_type != JoinType::INNER && comp_join.join_type != JoinType::SEMI &&
				           comp_join.join_type != JoinType::RIGHT_SEMI) {
					continue; // Unsupported join type
				}
				break;
			}
			case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
				if (comp_join.delim_flipped == 0) {
					(left_is_larger ? edge->protect_bigger_side : edge->protect_smaller_side) = true;
				} else {
					(left_is_larger ? edge->protect_smaller_side : edge->protect_bigger_side) = true;
				}
				break;
			}
			default:
				continue;
			}

			// Store edge info
			neighbor_matrix[left_table][right_table].push_back(edge);
			neighbor_matrix[right_table][left_table].push_back(edge);
		}
	}
}

// void TransferGraphManager::IgnoreUnfilteredTable() {
// 	size_t num_table = table_operator_manager.table_operators.size();
//
// 	vector<idx_t> unfiltered_table_idx;
// 	for (auto &pair : table_operator_manager.table_operators) {
// 		auto idx = pair.first;
// 		auto *table = pair.second;
//
// 		if (table->type != LogicalOperatorType::LOGICAL_GET) {
// 			continue;
// 		}
//
// 		auto &get = table->Cast<LogicalGet>();
// 		if (get.table_filters.filters.empty()) {
// 			unfiltered_table_idx.push_back(idx);
// 		}
// 	}
//
// 	for (auto &table_idx : unfiltered_table_idx) {
// 		vector<shared_ptr<EdgeInfo>> edges;
// 	}
// }

void TransferGraphManager::LargestRoot(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<idx_t> constructed_set;
	unordered_set<idx_t> unconstructed_set;
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	idx_t root = std::numeric_limits<idx_t>::max();

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
		if (selected_edge.first == std::numeric_limits<idx_t>::max()) {
			break;
		}

		auto &edges = neighbor_matrix[selected_edge.first][selected_edge.second];
		for (auto &edge : edges) {
			selected_edges.emplace_back(std::move(edge));
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

pair<idx_t, idx_t> TransferGraphManager::FindEdge(const unordered_set<idx_t> &constructed_set,
                                                  const unordered_set<idx_t> &unconstructed_set) {
	idx_t max_weight = 0, max_card = 0;
	pair<idx_t, idx_t> result {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()};

	for (auto i : unconstructed_set) {
		for (auto j : constructed_set) {
			auto key = make_pair(j, i);
			auto it = neighbor_matrix[i][j];
			if (it.empty()) {
				continue;
			}

			idx_t card = table_operator_manager.GetTableOperator(i)->estimated_cardinality;
			idx_t weight = it.size();

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
