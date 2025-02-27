#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"

#include <set>

namespace duckdb {
std::unordered_map<std::string, int> PredicateTransferOptimizer::table_exists;

unique_ptr<LogicalOperator> PredicateTransferOptimizer::PreOptimize(unique_ptr<LogicalOperator> plan,
                                                                    optional_ptr<RelationStats> stats) {
	dag_manager.Build(*plan);
	return plan;
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                                 optional_ptr<RelationStats> stats) {
	auto &ordered_nodes = dag_manager.getExecOrder();

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
			dag_manager.Add(BF.first, BF.second, false);
		}
	}

	// Backward
	for (int i = 0; i < ordered_nodes.size(); i++) {
		auto &current_node = ordered_nodes[i];
		auto BFs = CreateBloomFilter(*current_node, true);
		for (auto &BF : BFs) {
			dag_manager.Add(BF.first, BF.second, true);
		}
	}
	auto result = InsertCreateBFOperator(std::move(plan));
	return result;
}

/* which column(s) involved in this expression */
void PredicateTransferOptimizer::GetColumnBindingExpression(Expression &expr,
                                                            vector<BoundColumnRefExpression *> &expressions) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		Expression *expr_ptr = &expr;
		BoundColumnRefExpression *colref = static_cast<BoundColumnRefExpression *>(expr_ptr);
		D_ASSERT(colref->depth == 0);
		expressions.emplace_back(colref);
	} else {
		ExpressionIterator::EnumerateChildren(
		    expr, [&](unique_ptr<Expression> &child) { GetColumnBindingExpression(*child, expressions); });
	}
}

/* Create Bloom filter and use existing Bloom filter for the given scan or filter node */
vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node,
                                                                                                  bool reverse) {
	vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> result;
	vector<shared_ptr<BlockedBloomFilter>> bfs_to_use;
	vector<shared_ptr<BlockedBloomFilter>> bfs_to_create;

	idx_t node_id = GetNodeId(node);
	if (dag_manager.nodes.find(node_id) == dag_manager.nodes.end()) {
		return result;
	}

	// Use Bloom Filter
	vector<idx_t> parent_nodes;
	GetAllBFsToUse(node_id, bfs_to_use, parent_nodes, reverse);

	// Create Bloom Filter
	GetAllBFsToCreate(node_id, bfs_to_create, reverse);

	auto &operators = reverse ? replace_map_backward : replace_map_forward;

	if (!bfs_to_use.empty() && !bfs_to_create.empty()) {
		auto last_use_bf = BuildUseBFOperator(node, bfs_to_use, parent_nodes, reverse);

		auto create_bf = BuildCreateBFOperator(node, bfs_to_create);
		for (auto &filter : create_bf->bf_to_create) {
			result.emplace_back(make_pair(node_id, filter));
		}

		create_bf->AddChild(unique_ptr_cast<LogicalUseBF, LogicalOperator>(std::move(last_use_bf)));
		operators[&node] = std::move(create_bf);
	} else if (!bfs_to_use.empty()) {
		auto last_use_bf = BuildUseBFOperator(node, bfs_to_use, parent_nodes, reverse);

		operators[&node] = std::move(last_use_bf);
	} else if (!bfs_to_create.empty()) {
		if (!PossibleFilterAny(node, reverse)) {
			return result;
		}

		auto create_bf = BuildCreateBFOperator(node, bfs_to_create);
		for (auto &filter : create_bf->bf_to_create) {
			result.emplace_back(make_pair(node_id, filter));
		}

		operators[&node] = std::move(create_bf);
	}

	return result;
}

idx_t PredicateTransferOptimizer::GetNodeId(LogicalOperator &node) {
	idx_t res = -1;
	switch (node.type) {
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		res = node.GetTableIndex()[0];
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		res = NodesManager::GetTableIndexinFilter(&node);
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		res = node.GetTableIndex()[1];
		break;
	}
	default: {
		break;
	}
	}
	return res;
}

void PredicateTransferOptimizer::GetAllBFsToUse(idx_t cur_node_id, vector<shared_ptr<BlockedBloomFilter>> &bfs_to_use,
                                                vector<idx_t> &parent_nodes, bool reverse) {
	auto &node = dag_manager.nodes[cur_node_id];
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

void PredicateTransferOptimizer::GetAllBFsToCreate(idx_t cur_node_id,
                                                   vector<shared_ptr<BlockedBloomFilter>> &bfs_to_create,
                                                   bool reverse) {
	auto &node = dag_manager.nodes[cur_node_id];
	auto &edges = reverse ? node->backward_out_ : node->forward_out_;

	for (auto &edge : edges) {
		auto cur_filter = make_shared_ptr<BlockedBloomFilter>();

		// Each expression leads to a bloom filter on a column on this table
		for (auto &expr : edge->filters) {
			vector<BoundColumnRefExpression *> expressions;
			GetColumnBindingExpression(*expr, expressions);
			D_ASSERT(expressions.size() == 2);

			auto binding0 = dag_manager.nodes_manager.FindRename(expressions[0]->binding);
			auto binding1 = dag_manager.nodes_manager.FindRename(expressions[1]->binding);

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

unique_ptr<LogicalCreateBF>
PredicateTransferOptimizer::BuildCreateBFOperator(LogicalOperator &node,
                                                  vector<shared_ptr<BlockedBloomFilter>> &bloom_filters) {
	auto create_bf = make_uniq<LogicalCreateBF>(bloom_filters);
	create_bf->SetEstimatedCardinality(node.estimated_cardinality);
	return create_bf;
}

unique_ptr<LogicalUseBF>
PredicateTransferOptimizer::BuildUseBFOperator(LogicalOperator &node,
                                               vector<shared_ptr<BlockedBloomFilter>> &bloom_filters,
                                               vector<idx_t> &parent_nodes, bool reverse) {
	unique_ptr<LogicalUseBF> last_operator;
	auto &operators = reverse ? replace_map_backward : replace_map_forward;

	// This is important for performance, not use (int i = 0; i < temp_result_to_use.size(); i++)
	for (int i = bloom_filters.size() - 1; i >= 0; i--) {
		auto use_bf_operator = make_uniq<LogicalUseBF>(bloom_filters[i]);
		use_bf_operator->SetEstimatedCardinality(node.estimated_cardinality);

		auto node_id = parent_nodes[i];
		auto base_node = dag_manager.nodes_manager.GetNode(node_id);
		auto related_bf_create = operators[base_node].get();

		D_ASSERT(related_bf_create->type == LogicalOperatorType::LOGICAL_CREATE_BF);
		use_bf_operator->AddDownStreamOperator(static_cast<LogicalCreateBF *>(related_bf_create));

		if (last_operator != nullptr) {
			use_bf_operator->AddChild(std::move(last_operator));
		}
		last_operator = std::move(use_bf_operator);
	}
	return last_operator;
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertCreateBFOperator(unique_ptr<LogicalOperator> plan) {
	for (auto &child : plan->children) {
		child = InsertCreateBFOperator(std::move(child));
	}
	void *plan_ptr = plan.get();
	auto itr = replace_map_forward.find(plan_ptr);
	bool insert_create_table = false;
	if (itr != replace_map_forward.end()) {
		insert_create_table = true;
		auto ptr = itr->second.get();
		while (ptr->children.size() != 0) {
			ptr = ptr->children[0].get();
		}
		ptr->AddChild(std::move(plan));
		plan = std::move(itr->second);
	}
	auto itr_next = replace_map_backward.find(plan_ptr);
	if (itr_next != replace_map_backward.end()) {
		insert_create_table = true;
		auto ptr_next = itr_next->second.get();
		while (ptr_next->children.size() != 0) {
			ptr_next = ptr_next->children[0].get();
		}
		ptr_next->AddChild(std::move(plan));
		plan = std::move(itr_next->second);
	}
	/*
	if (insert_create_table) {
	    plan = InsertCreateTable(std::move(plan), (LogicalOperator*)plan_ptr);
	}
	*/
	return plan;
}

/* Will this node be filtered? */
bool PredicateTransferOptimizer::PossibleFilterAny(LogicalOperator &node, bool reverse) {
	if (!reverse || (replace_map_forward.find(&node) == replace_map_forward.end())) {
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
} // namespace duckdb