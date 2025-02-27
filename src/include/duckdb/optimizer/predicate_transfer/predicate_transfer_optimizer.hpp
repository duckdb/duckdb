#pragma once

#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"

namespace duckdb {
class PredicateTransferOptimizer {
public:
	explicit PredicateTransferOptimizer(ClientContext &context) : context(context), dag_manager(context) {
	}

	unique_ptr<LogicalOperator> PreOptimize(unique_ptr<LogicalOperator> plan,
	                                        optional_ptr<RelationStats> stats = nullptr);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats = nullptr);
	unique_ptr<LogicalOperator> InsertCreateBFOperator(unique_ptr<LogicalOperator> plan);

private:
	vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> CreateBloomFilter(LogicalOperator &node, bool reverse);

	void GetAllBFsToUse(idx_t cur_node_id, vector<shared_ptr<BlockedBloomFilter>> &bfs_to_use,
	                    vector<idx_t> &parent_nodes, bool reverse);
	void GetAllBFsToCreate(idx_t cur_node_id, vector<shared_ptr<BlockedBloomFilter>> &bfs_to_create, bool reverse);

	unique_ptr<LogicalCreateBF> BuildCreateBFOperator(LogicalOperator &node,
	                                                  vector<shared_ptr<BlockedBloomFilter>> &bloom_filters);
	unique_ptr<LogicalUseBF> BuildUseBFOperator(LogicalOperator &node,
	                                            vector<shared_ptr<BlockedBloomFilter>> &bloom_filters,
	                                            vector<idx_t> &parent_nodes, bool reverse);

	bool PossibleFilterAny(LogicalOperator &node, bool reverse);

	static void GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression *> &expressions);
	static idx_t GetNodeId(LogicalOperator &node);

private:
	ClientContext &context;
	DAGManager dag_manager;

	std::unordered_map<void *, unique_ptr<LogicalOperator>> replace_map_forward;
	std::unordered_map<void *, unique_ptr<LogicalOperator>> replace_map_backward;
	static std::unordered_map<std::string, int> table_exists;
};
} // namespace duckdb