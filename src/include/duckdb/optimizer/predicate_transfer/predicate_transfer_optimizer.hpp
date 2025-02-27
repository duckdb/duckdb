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
	void GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression *> &expressions);
	vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> CreateBloomFilter(LogicalOperator &node, bool reverse);

	void GetAllBFUsed(idx_t cur, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use,
	                  vector<idx_t> &depend_nodes, bool reverse);
	void GetAllBFCreate(idx_t cur, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_create, bool reverse);

	unique_ptr<LogicalCreateBF>
	BuildSingleCreateOperator(LogicalOperator &node, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_create);

	unique_ptr<LogicalUseBF> BuildUseOperator(LogicalOperator &node,
	                                          vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use,
	                                          vector<idx_t> &depend_nodes, bool reverse);

	unique_ptr<LogicalCreateBF> BuildCreateUsePair(LogicalOperator &node,
	                                               vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use,
	                                               vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_create,
	                                               vector<idx_t> &depend_nodes, bool reverse);

	idx_t GetNodeId(LogicalOperator &node);

	bool PossibleFilterAny(LogicalOperator &node, bool reverse);

private:
	ClientContext &context;
	DAGManager dag_manager;

	std::unordered_map<void *, unique_ptr<LogicalOperator>> replace_map_forward;
	std::unordered_map<void *, unique_ptr<LogicalOperator>> replace_map_backward;
	static std::unordered_map<std::string, int> table_exists;
};
} // namespace duckdb