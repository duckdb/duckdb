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
		for (auto &BF_plan : CreateBloomFilterPlan(*current_node, false)) {
			graph_manager.AddFilterPlan(BF_plan.first, BF_plan.second, false);
		}
	}

	// **Backward pass**: Process nodes in original order (from first to last)
	// - Similar to the forward pass, but for backward edges
	for (auto *current_node : ordered_nodes) {
		for (auto &BF_plan : CreateBloomFilterPlan(*current_node, true)) {
			graph_manager.AddFilterPlan(BF_plan.first, BF_plan.second, true);
		}
	}

	return InsertTransferOperators(std::move(plan));
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertTransferOperators(unique_ptr<LogicalOperator> plan) {
	for (auto &child : plan->children) {
		child = InsertTransferOperators(std::move(child));
	}

	// Store original operator pointer
	LogicalOperator *original_operator = plan.get();
	auto apply_modification = [&](std::unordered_map<LogicalOperator *, unique_ptr<LogicalOperator>> &modify_map) {
		auto it = modify_map.find(original_operator);
		if (it == modify_map.end()) {
			// No modification needed
			return;
		}

		auto *last_op = it->second.get();
		auto *last_creator = (it->second->type == LogicalOperatorType::LOGICAL_CREATE_BF) ? it->second.get() : nullptr;
		while (!last_op->children.empty()) {
			last_op = last_op->children[0].get();
			if (last_op->type == LogicalOperatorType::LOGICAL_CREATE_BF) {
				last_creator = last_op;
			}
		}

		// table scan
		if (last_creator == last_op || plan->type != LogicalOperatorType::LOGICAL_FILTER) {
			last_op->AddChild(std::move(plan));
			plan = std::move(it->second);
			return;
		}

		// creator ---> user --> creator --> filter --> user --> user --> scan
		last_op->AddChild(std::move(plan->children[0]));
		if (last_creator == nullptr) {
			plan->children[0] = std::move(it->second);
		} else {
			plan->children[0] = std::move(last_creator->children[0]);
			last_creator->children[0] = std::move(plan);
			plan = std::move(it->second);
		}
	};

	apply_modification(forward_stage_modification);
	apply_modification(backward_stage_modification);

	return plan;
}

vector<pair<idx_t, shared_ptr<FilterPlan>>> PredicateTransferOptimizer::CreateBloomFilterPlan(LogicalOperator &node,
                                                                                              bool reverse) {
	vector<pair<idx_t, shared_ptr<FilterPlan>>> result;

	vector<shared_ptr<FilterPlan>> bfs_to_use_plan;
	vector<shared_ptr<FilterPlan>> bfs_to_create_plan;

	idx_t node_id = TableOperatorManager::GetScalarTableIndex(&node);
	if (node_id == std::numeric_limits<idx_t>::max() ||
	    graph_manager.transfer_graph.find(node_id) == graph_manager.transfer_graph.end()) {
		return result;
	}

	// Use Bloom Filter
	vector<idx_t> parent_nodes;
	GetAllBFsToUse(node_id, bfs_to_use_plan, parent_nodes, reverse);

	// Create Bloom Filter
	GetAllBFsToCreate(node_id, bfs_to_create_plan, reverse);

	auto &replace_map = reverse ? backward_stage_modification : forward_stage_modification;

	if (!bfs_to_use_plan.empty() && !bfs_to_create_plan.empty()) {
		auto last_use_bf = BuildUseBFOperator(node, bfs_to_use_plan);
		auto create_bf = BuildCreateBFOperator(node, bfs_to_create_plan);
		for (auto &filter : create_bf->filter_plans) {
			result.emplace_back(make_pair(node_id, filter));
		}
		create_bf->AddChild(unique_ptr_cast<LogicalUseBF, LogicalOperator>(std::move(last_use_bf)));
		replace_map[&node] = std::move(create_bf);
	} else if (!bfs_to_use_plan.empty()) {
		auto last_use_bf = BuildUseBFOperator(node, bfs_to_use_plan);
		replace_map[&node] = std::move(last_use_bf);
	} else if (!bfs_to_create_plan.empty()) {
		if (!HasAnyFilter(node, reverse)) {
			return result;
		}

		auto create_bf = BuildCreateBFOperator(node, bfs_to_create_plan);
		for (auto &filter : create_bf->filter_plans) {
			result.emplace_back(make_pair(node_id, filter));
		}
		replace_map[&node] = std::move(create_bf);
	}

	return result;
}

void PredicateTransferOptimizer::GetAllBFsToUse(idx_t cur_node_id, vector<shared_ptr<FilterPlan>> &bfs_to_use_plan,
                                                vector<idx_t> &parent_nodes, bool reverse) {
	auto &node = graph_manager.transfer_graph[cur_node_id];
	auto &edges = reverse ? node->backward_stage_edges.in : node->forward_stage_edges.in;

	for (auto &edge : edges) {
		for (auto &bf : edge->filter_plan) {
			if (std::find(bfs_to_use_plan.begin(), bfs_to_use_plan.end(), bf) == bfs_to_use_plan.end()) {
				bfs_to_use_plan.emplace_back(bf);
				parent_nodes.emplace_back(edge->destination);
			}
		}
	}
}

void PredicateTransferOptimizer::GetAllBFsToCreate(idx_t cur_node_id,
                                                   vector<shared_ptr<FilterPlan>> &bfs_to_create_plan, bool reverse) {
	auto &node = graph_manager.transfer_graph[cur_node_id];
	auto &edges = reverse ? node->backward_stage_edges.out : node->forward_stage_edges.out;

	for (auto &edge : edges) {
		auto bf_plan = make_shared_ptr<FilterPlan>();

		// Each expression leads to a bloom filter on a column on this table
		for (auto &expr : edge->conditions) {
			vector<BoundColumnRefExpression *> expressions;
			GetColumnBindingExpression(*expr, expressions);
			D_ASSERT(expressions.size() == 2);

			auto binding0 = graph_manager.table_operator_manager.GetRenaming(expressions[0]->binding);
			auto binding1 = graph_manager.table_operator_manager.GetRenaming(expressions[1]->binding);

			expressions[0]->binding = binding0;
			expressions[1]->binding = binding1;

			if (binding0.table_index == cur_node_id) {
				bf_plan->build.push_back(expressions[0]->Copy());
				bf_plan->apply.push_back(expressions[1]->Copy());
			} else if (binding1.table_index == cur_node_id) {
				bf_plan->build.push_back(expressions[1]->Copy());
				bf_plan->apply.push_back(expressions[0]->Copy());
			}
		}
		if (!bf_plan->build.empty()) {
			bfs_to_create_plan.emplace_back(std::move(bf_plan));
		} else {
			throw InternalException("No built column found!");
		}
	}
}

unique_ptr<LogicalCreateBF>
PredicateTransferOptimizer::BuildCreateBFOperator(LogicalOperator &node, vector<shared_ptr<FilterPlan>> &bf_plans) {
	auto create_bf = make_uniq<LogicalCreateBF>(bf_plans);
	create_bf->SetEstimatedCardinality(node.estimated_cardinality);
	return create_bf;
}

unique_ptr<LogicalUseBF> PredicateTransferOptimizer::BuildUseBFOperator(LogicalOperator &node,
                                                                        vector<shared_ptr<FilterPlan>> &bf_plans) {
	unique_ptr<LogicalUseBF> last_operator;

	// This is important for performance, not use (int i = 0; i < temp_result_to_use.size(); i++)
	for (auto it = bf_plans.rbegin(); it != bf_plans.rend(); ++it) {
		auto use_bf_operator = make_uniq<LogicalUseBF>(*it);
		use_bf_operator->SetEstimatedCardinality(node.estimated_cardinality);
		if (last_operator) {
			use_bf_operator->AddChild(std::move(last_operator));
		}
		last_operator = std::move(use_bf_operator);
	}

	return last_operator;
}

bool PredicateTransferOptimizer::HasAnyFilter(LogicalOperator &node, bool reverse) {
	if (!reverse || (forward_stage_modification.find(&node) == forward_stage_modification.end())) {
		if (node.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = node.Cast<LogicalGet>();
			if (get.table_filters.filters.empty()) {
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
