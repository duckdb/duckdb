#pragma once

#include "duckdb/optimizer/predicate_transfer/transfer_graph_manager.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"

namespace duckdb {
using BloomFilters = vector<shared_ptr<BlockedBloomFilter>>;

class PredicateTransferOptimizer {
public:
	explicit PredicateTransferOptimizer(ClientContext &context) : context(context), graph_manager(context) {
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
	vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> CreateBloomFilter(LogicalOperator &node, bool reverse = false);

	void GetAllBFsToUse(idx_t cur_node_id, BloomFilters &bfs_to_use, vector<idx_t> &parent_nodes, bool reverse = false);
	void GetAllBFsToCreate(idx_t cur_node_id, BloomFilters &bfs_to_create, bool reverse = false);
	unique_ptr<LogicalCreateBF> BuildCreateBFOperator(LogicalOperator &node, BloomFilters &bloom_filters);
	unique_ptr<LogicalUseBF> BuildUseBFOperator(LogicalOperator &node, BloomFilters &bloom_filters,
	                                            vector<idx_t> &parent_nodes, bool reverse = false);

	//! Will this node be filtered?
	bool PossibleFilterAny(LogicalOperator &node, bool reverse = false);

	//! which column(s) involved in this expression
	static void GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression *> &expressions);

private:
	ClientContext &context;
	TransferGraphManager graph_manager;

	//! we use a map to record how to modify/update the operators in the query plan.
	std::unordered_map<LogicalOperator *, unique_ptr<LogicalOperator>> modify_map_for_forward_stage;
	std::unordered_map<LogicalOperator *, unique_ptr<LogicalOperator>> modify_map_for_backward_stage;
};
} // namespace duckdb
