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
	auto &ordered_nodes = graph_manager.GetExecutionOrder();

	// Forward
	for (int i = ordered_nodes.size() - 1; i >= 0; i--) {
		auto current_node = ordered_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return BF and its corresponding table id
		auto BFvec = CreateBloomFilter(*current_node, false);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
			graph_manager.Add(BF.first, BF.second, false);
		}
	}

	// Backward
	for (int i = 0; i < ordered_nodes.size(); i++) {
		auto &current_node = ordered_nodes[i];
		auto BFs = CreateBloomFilter(*current_node, true);
		for (auto &BF : BFs) {
			graph_manager.Add(BF.first, BF.second, true);
		}
	}
	auto result = InsertTransferOperators(std::move(plan));
	return result;
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertTransferOperators(unique_ptr<LogicalOperator> plan) {
	for (auto &child : plan->children) {
		child = InsertTransferOperators(std::move(child));
	}

	auto *operator_ptr = plan.get();
	auto itr = modify_map_forward.find(operator_ptr);
	if (itr != modify_map_forward.end()) {
		auto ptr = itr->second.get();
		while (ptr->children.size() != 0) {
			ptr = ptr->children[0].get();
		}
		ptr->AddChild(std::move(plan));
		plan = std::move(itr->second);
	}

	D_ASSERT(operator_ptr != nullptr);
	auto itr_next = modify_map_backward.find(operator_ptr);
	if (itr_next != modify_map_backward.end()) {
		auto ptr_next = itr_next->second.get();
		while (ptr_next->children.size() != 0) {
			ptr_next = ptr_next->children[0].get();
		}
		ptr_next->AddChild(std::move(plan));
		plan = std::move(itr_next->second);
	}

	return plan;
}

vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node,
                                                                                                  bool reverse) {
	vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> result;
	BloomFilters bfs_to_use;
	BloomFilters bfs_to_create;

	idx_t node_id = NodesManager::GetScalarTableIndex(&node);
	if (node_id == -1 || graph_manager.graph.find(node_id) == graph_manager.graph.end()) {
		return result;
	}

	// Use Bloom Filter
	vector<idx_t> parent_nodes;
	GetAllBFsToUse(node_id, bfs_to_use, parent_nodes, reverse);

	// Create Bloom Filter
	GetAllBFsToCreate(node_id, bfs_to_create, reverse);

	auto &replace_map = reverse ? modify_map_backward : modify_map_forward;

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
	auto &node = graph_manager.graph[cur_node_id];
	auto &edges = reverse ? node->backward_in_ : node->forward_in_;

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
	auto &node = graph_manager.graph[cur_node_id];
	auto &edges = reverse ? node->backward_out_ : node->forward_out_;

	for (auto &edge : edges) {
		auto cur_filter = make_shared_ptr<BlockedBloomFilter>();

		// Each expression leads to a bloom filter on a column on this table
		for (auto &expr : edge->filters) {
			vector<BoundColumnRefExpression *> expressions;
			GetColumnBindingExpression(*expr, expressions);
			D_ASSERT(expressions.size() == 2);

			auto binding0 = graph_manager.nodes_manager.FindRename(expressions[0]->binding);
			auto binding1 = graph_manager.nodes_manager.FindRename(expressions[1]->binding);

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
	auto &replace_map = reverse ? modify_map_backward : modify_map_forward;

	// This is important for performance, not use (int i = 0; i < temp_result_to_use.size(); i++)
	for (int i = bloom_filters.size() - 1; i >= 0; i--) {
		auto use_bf_operator = make_uniq<LogicalUseBF>(bloom_filters[i]);
		use_bf_operator->SetEstimatedCardinality(node.estimated_cardinality);

		auto node_id = parent_nodes[i];
		auto base_node = graph_manager.nodes_manager.GetNode(node_id);
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
	if (!reverse || (modify_map_forward.find(&node) == modify_map_forward.end())) {
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