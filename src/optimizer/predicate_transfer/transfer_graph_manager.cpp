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

			hash_t hash = ComputeConditionHash(cond);
			if (!existed_set.insert(hash).second) {
				continue;
			}

			auto &left_col_expression = cond.left->Cast<BoundColumnRefExpression>();
			ColumnBinding left_binding = table_operator_manager.GetRenaming(left_col_expression.binding);
			auto left_node = table_operator_manager.GetTableOperator(left_binding.table_index);

			auto &right_col_expression = cond.right->Cast<BoundColumnRefExpression>();
			ColumnBinding right_binding = table_operator_manager.GetRenaming(right_col_expression.binding);
			auto right_node = table_operator_manager.GetTableOperator(right_binding.table_index);

			if (!left_node || !right_node) {
				continue;
			}

			// Create edge
			auto expr = make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
			auto edge =
			    make_shared_ptr<EdgeInfo>(std::move(expr), *left_node, left_binding, *right_node, right_binding);

			// Set protection flags
			switch (comp_join.type) {
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
				if (comp_join.join_type == JoinType::LEFT) {
					edge->protect_left = true;
				} else if (comp_join.join_type == JoinType::MARK) {
					edge->protect_right = true;
				} else if (comp_join.join_type == JoinType::RIGHT) {
					edge->protect_right = true;
				} else if (comp_join.join_type != JoinType::INNER && comp_join.join_type != JoinType::SEMI &&
				           comp_join.join_type != JoinType::RIGHT_SEMI) {
					continue; // Unsupported join type
				}
				break;
			}
			case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
				if (comp_join.delim_flipped == 0) {
					edge->protect_left = true;
				} else {
					edge->protect_right = true;
				}
				break;
			}
			default:
				continue;
			}

			// Store edge info
			neighbor_matrix[left_binding.table_index][right_binding.table_index].push_back(edge);
			neighbor_matrix[right_binding.table_index][left_binding.table_index].push_back(edge);
		}
	}
}

void TransferGraphManager::IgnoreUnfilteredTable() {
	size_t num_table = table_operator_manager.table_operators.size();

	// collect unfiltered tables
	vector<idx_t> unfiltered_table_idx;
	for (auto &pair : table_operator_manager.table_operators) {
		auto idx = pair.first;
		auto *table = pair.second;

		if (table->type != LogicalOperatorType::LOGICAL_GET) {
			continue;
		}

		auto &get = table->Cast<LogicalGet>();
		if (get.table_filters.filters.empty()) {
			unfiltered_table_idx.push_back(idx);
		}
	}

	for (auto &table_idx : unfiltered_table_idx) {
		// collect received BFs for this table
		unordered_map<idx_t, vector<shared_ptr<EdgeInfo>>> received_BFs;

		auto &edges = neighbor_matrix[table_idx];
		for (auto &pair : edges) {
			auto creator_table_idx = pair.first;
			auto &edge = pair.second;

			D_ASSERT(edge.size() == 1);
			for (auto &e : edge) {
				if (e->left_binding.table_index == table_idx && !e->protect_left) {

				} else if (e->right_binding.table_index == table_idx && !e->protect_right) {
				}
			}
		}
	}
}

void TransferGraphManager::LargestRoot(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<idx_t> constructed_set, unconstructed_set;
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	idx_t root = std::numeric_limits<idx_t>::max();

	// Initialize nodes
	for (auto &entry : table_operator_manager.table_operators) {
		idx_t id = entry.first;
		auto node = make_uniq<GraphNode>(id, prior_flag--);

		if (entry.second == sorted_nodes.back()) {
			root = id;
			constructed_set.insert(id);
		} else {
			unconstructed_set.insert(id);
		}

		transfer_graph[id] = std::move(node);
	}

	// Add root
	transfer_order.push_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);

	// Build graph
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

		transfer_order.push_back(table_operator_manager.GetTableOperator(node->id));
		table_operator_manager.table_operators.erase(node->id);
		unconstructed_set.erase(selected_edge.second);
		constructed_set.insert(selected_edge.second);
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

		idx_t left_idx = TableOperatorManager::GetScalarTableIndex(&edge->left_table);
		idx_t right_idx = TableOperatorManager::GetScalarTableIndex(&edge->right_table);

		D_ASSERT(left_idx != std::numeric_limits<idx_t>::max() && right_idx != std::numeric_limits<idx_t>::max());

		auto left_node = transfer_graph[right_idx].get();
		auto right_node = transfer_graph[left_idx].get();
		bool swap_order = (left_node->priority > right_node->priority);
		auto *condition = edge->condition.get();

		// forward: from the smaller to the larger
		if (!edge->protect_left) {
			left_node->Add(right_node->id, condition, !swap_order, false);
			right_node->Add(left_node->id, condition, !swap_order, true);
		}

		// backward: from the larger to the smaller
		if (!edge->protect_right) {
			left_node->Add(right_node->id, condition, swap_order, true);
			right_node->Add(left_node->id, condition, swap_order, false);
		}
	}
}

pair<idx_t, idx_t> TransferGraphManager::FindEdge(const unordered_set<idx_t> &constructed_set,
                                                  const unordered_set<idx_t> &unconstructed_set) {
	pair<idx_t, idx_t> result {std::numeric_limits<idx_t>::max(), std::numeric_limits<idx_t>::max()};
	idx_t max_weight = 0, max_card = 0;

	for (auto i : unconstructed_set) {
		for (auto j : constructed_set) {
			auto &edges = neighbor_matrix[i][j];
			if (edges.empty()) {
				continue;
			}

			idx_t card = table_operator_manager.GetTableOperator(i)->estimated_cardinality;
			idx_t weight = edges.size();

			if (weight > max_weight || (weight == max_weight && card > max_card)) {
				max_weight = weight;
				max_card = card;
				result = {j, i};
			}
		}
	}
	return result;
}

} // namespace duckdb
