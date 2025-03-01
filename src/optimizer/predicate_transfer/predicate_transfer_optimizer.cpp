#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> PredicateTransferOptimizer::PreOptimize(unique_ptr<LogicalOperator> plan) {
	graph_manager.Build(*plan);
	return plan;
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	auto &ordered_nodes = graph_manager.transfer_order;

	// **Forward pass**: Process nodes in reverse order (from last to first)
	// - Generate Bloom Filters (BFs) based on predicates
	// - Add BFs to the corresponding edges in the graph
	for (auto it = ordered_nodes.rbegin(); it != ordered_nodes.rend(); ++it) {
		auto *current_node = *it;
		for (auto &BF : CreateBloomFilter(*current_node, false)) {
			graph_manager.AddBloomFilter(BF.first, BF.second, false);
		}
	}

	// **Backward pass**: Process nodes in original order (from first to last)
	// - Similar to the forward pass, but for backward edges
	for (auto *current_node : ordered_nodes) {
		for (auto &BF : CreateBloomFilter(*current_node, true)) {
			graph_manager.AddBloomFilter(BF.first, BF.second, true);
		}
	}

	return InsertTransferOperators(std::move(plan));
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertTransferOperators(unique_ptr<LogicalOperator> plan) {
	for (auto &child : plan->children) {
		child = InsertTransferOperators(std::move(child));
	}

	LogicalOperator *original_operator = plan.get(); // Store original operator pointer

	auto apply_modification = [&](std::unordered_map<LogicalOperator *, unique_ptr<LogicalOperator>> &modify_map) {
		auto it = modify_map.find(original_operator);
		if (it == modify_map.end()) {
			return; // No modification needed
		}

		auto *last_child = it->second.get();
		while (!last_child->children.empty()) {
			last_child = last_child->children[0].get();
		}
		last_child->AddChild(std::move(plan));
		plan = std::move(it->second);
	};

	apply_modification(modify_map_for_forward_stage);
	apply_modification(modify_map_for_backward_stage);

	return plan;
}

vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node,
                                                                                                  bool reverse) {
	vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> result;
	BloomFilters bfs_to_use;
	BloomFilters bfs_to_create;

	idx_t node_id = TableOperatorManager::GetScalarTableIndex(&node);
	if (node_id == -1 || graph_manager.transfer_graph.find(node_id) == graph_manager.transfer_graph.end()) {
		return result;
	}

	// Use Bloom Filter
	vector<idx_t> parent_nodes;
	GetAllBFsToUse(node_id, bfs_to_use, parent_nodes, reverse);

	// Create Bloom Filter
	GetAllBFsToCreate(node_id, bfs_to_create, reverse);

	auto &replace_map = reverse ? modify_map_for_backward_stage : modify_map_for_forward_stage;

	if (!bfs_to_use.empty() && !bfs_to_create.empty()) {
		auto last_use_bf = BuildUseBFOperator(node, bfs_to_use, parent_nodes, reverse);
		auto create_bf = BuildCreateBFOperator(node, bfs_to_create);
		for (auto &filter : create_bf->bf_to_create) {
			result.emplace_back(make_pair(node_id, filter));
		}
		create_bf->AddChild(unique_ptr_cast<LogicalUseBF, LogicalOperator>(std::move(last_use_bf)));
		replace_map[&node] = std::move(create_bf);
	} else if (!bfs_to_use.empty()) {
		auto last_use_bf = BuildUseBFOperator(node, bfs_to_use, parent_nodes, reverse);
		replace_map[&node] = std::move(last_use_bf);
	} else if (!bfs_to_create.empty()) {
		if (!PossibleFilterAny(node, reverse)) {
			return result;
		}

		auto create_bf = BuildCreateBFOperator(node, bfs_to_create);
		for (auto &filter : create_bf->bf_to_create) {
			result.emplace_back(make_pair(node_id, filter));
		}
		replace_map[&node] = std::move(create_bf);
	}

	return result;
}

void PredicateTransferOptimizer::GetAllBFsToUse(idx_t cur_node_id, BloomFilters &bfs_to_use,
                                                vector<idx_t> &parent_nodes, bool reverse) {
	auto &node = graph_manager.transfer_graph[cur_node_id];
	auto &edges = reverse ? node->backward_stage_edges.in : node->forward_stage_edges.in;

	for (auto &edge : edges) {
		for (auto bf : edge->bloom_filters) {
			if (!bf->isUsed()) {
				bf->setUsed();
				bfs_to_use.emplace_back(bf);
				parent_nodes.emplace_back(edge->destination);
			}
		}
	}
}

void PredicateTransferOptimizer::GetAllBFsToCreate(idx_t cur_node_id, BloomFilters &bfs_to_create, bool reverse) {
	auto &node = graph_manager.transfer_graph[cur_node_id];
	auto &edges = reverse ? node->backward_stage_edges.out : node->forward_stage_edges.out;

	for (auto &edge : edges) {
		auto cur_filter = make_shared_ptr<BlockedBloomFilter>();

		// Each expression leads to a bloom filter on a column on this table
		for (auto &expr : edge->filters) {
			vector<BoundColumnRefExpression *> expressions;
			GetColumnBindingExpression(*expr, expressions);
			D_ASSERT(expressions.size() == 2);

			auto binding0 = graph_manager.table_operator_manager.GetRenaming(expressions[0]->binding);
			auto binding1 = graph_manager.table_operator_manager.GetRenaming(expressions[1]->binding);

			if (binding0.table_index == cur_node_id) {
				cur_filter->AddColumnBindingApplied(binding1);
				cur_filter->AddColumnBindingBuilt(binding0);
			} else if (binding1.table_index == cur_node_id) {
				cur_filter->AddColumnBindingApplied(binding0);
				cur_filter->AddColumnBindingBuilt(binding1);
			}
		}
		if (cur_filter->column_bindings_built_.size() != 0) {
			bfs_to_create.emplace_back(cur_filter);
		} else {
			throw InternalException("No built column found!");
		}
	}
}

unique_ptr<LogicalCreateBF> PredicateTransferOptimizer::BuildCreateBFOperator(LogicalOperator &node,
                                                                              BloomFilters &bloom_filters) {
	auto create_bf = make_uniq<LogicalCreateBF>(bloom_filters);
	create_bf->SetEstimatedCardinality(node.estimated_cardinality);
	return create_bf;
}

unique_ptr<LogicalUseBF> PredicateTransferOptimizer::BuildUseBFOperator(LogicalOperator &node,
                                                                        BloomFilters &bloom_filters,
                                                                        vector<idx_t> &parent_nodes, bool reverse) {
	unique_ptr<LogicalUseBF> last_operator;
	auto &replace_map = reverse ? modify_map_for_backward_stage : modify_map_for_forward_stage;

	// This is important for performance, not use (int i = 0; i < temp_result_to_use.size(); i++)
	for (int i = bloom_filters.size() - 1; i >= 0; i--) {
		auto use_bf_operator = make_uniq<LogicalUseBF>(bloom_filters[i]);
		use_bf_operator->SetEstimatedCardinality(node.estimated_cardinality);

		auto node_id = parent_nodes[i];
		auto base_node = graph_manager.table_operator_manager.GetTableOperator(node_id);
		auto related_bf_create = replace_map[base_node].get();

		D_ASSERT(related_bf_create->type == LogicalOperatorType::LOGICAL_CREATE_BF);
		use_bf_operator->AddDownStreamOperator(static_cast<LogicalCreateBF *>(related_bf_create));

		if (last_operator != nullptr) {
			use_bf_operator->AddChild(std::move(last_operator));
		}
		last_operator = std::move(use_bf_operator);
	}
	return last_operator;
}

bool PredicateTransferOptimizer::PossibleFilterAny(LogicalOperator &node, bool reverse) {
	if (!reverse || (modify_map_for_forward_stage.find(&node) == modify_map_for_forward_stage.end())) {
		if (node.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = node.Cast<LogicalGet>();
			if (get.table_filters.filters.size() == 0) {
				return false;
			}
		} else if (node.type == LogicalOperatorType::LOGICAL_UNION) {
			return false;
		}
	}

	return true;
}

void PredicateTransferOptimizer::GetColumnBindingExpression(Expression &expr,
                                                            vector<BoundColumnRefExpression *> &expressions) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		Expression *expr_ptr = &expr;
		BoundColumnRefExpression *col_ref = static_cast<BoundColumnRefExpression *>(expr_ptr);
		D_ASSERT(col_ref->depth == 0);
		expressions.emplace_back(col_ref);
	} else {
		ExpressionIterator::EnumerateChildren(
		    expr, [&](unique_ptr<Expression> &child) { GetColumnBindingExpression(*child, expressions); });
	}
}
} // namespace duckdb