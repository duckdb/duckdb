#pragma once

#include "duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp"

namespace duckdb {
class LogicalCreateBF;
class LogicalUseBF;

class PredicateTransferOptimizer {
public:
	explicit PredicateTransferOptimizer(ClientContext &context) : graph_manager(context) {
	}

	//! Extract the query join information, note that this function must be called before join order optimization,
	//! because some join conditions are lost during join order optimization.
	unique_ptr<LogicalOperator> PreOptimize(unique_ptr<LogicalOperator> plan);

	//! Create bloom filters and insert them into the query plan, note that this function must be called after join
	//! order optimization, because it cannot handle newly inserted operator correctly.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	unique_ptr<LogicalOperator> InsertTransferOperators(unique_ptr<LogicalOperator> plan);

	//! Create Bloom filter and use existing Bloom filter for the given scan or filter node
	vector<pair<idx_t, shared_ptr<FilterPlan>>> CreateBloomFilterPlan(LogicalOperator &node, bool reverse = false);

	void GetAllBFsToUse(idx_t cur_node_id, vector<shared_ptr<FilterPlan>> &bfs_to_use_plan, vector<idx_t> &parent_nodes,
	                    bool reverse);
	void GetAllBFsToCreate(idx_t cur_node_id, vector<shared_ptr<FilterPlan>> &bfs_to_create_plan, bool reverse);

	static unique_ptr<LogicalCreateBF> BuildCreateBFOperator(LogicalOperator &node,
	                                                         vector<shared_ptr<FilterPlan>> &bf_plans);
	static unique_ptr<LogicalUseBF> BuildUseBFOperator(LogicalOperator &node, vector<shared_ptr<FilterPlan>> &bf_plans);

	bool HasAnyFilter(LogicalOperator &node, bool reverse = false);

	//! which column(s) involved in this expression?
	static void GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression *> &expressions);

private:
	TransferGraphManager graph_manager;

	//! we use a map to record how to modify/update the operators in the query plan.
	std::unordered_map<LogicalOperator *, unique_ptr<LogicalOperator>> forward_stage_modification;
	std::unordered_map<LogicalOperator *, unique_ptr<LogicalOperator>> backward_stage_modification;
};
} // namespace duckdb
