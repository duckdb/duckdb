#pragma once

#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"

namespace duckdb {
class PredicateTransferScheduler {
public:
	PredicateTransferScheduler(ClientContext &context) : context(context) {
	}

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op) {
		// extract all vertex nodes and joins
		vector<reference<LogicalOperator>> tables;
		vector<reference<LogicalOperator>> joins;
		ExtractNodes(*op, tables, joins);

		vector<reference<LogicalOperator>> sorted_tables = tables;
		sort(sorted_tables.begin(), sorted_tables.end(),
		     [](reference<LogicalOperator> a, reference<LogicalOperator> b) {
		     	return a.get().estimated_cardinality < b.get().estimated_cardinality;
		     });

		// transform joins into edges

		return op;
	}

private:
	ClientContext &context;

	static void ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &tables,
	                         vector<reference<LogicalOperator>> &joins);

	static void TransformEdges(vector<reference<LogicalOperator>> &joins) {
		expression_set_t filter_set;

		for (auto &join : joins) {
			D_ASSERT(join.get().type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			         join.get().type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
			D_ASSERT(join.get().expressions.empty());

			auto &join_expressions = join.get().Cast<LogicalComparisonJoin>().conditions;
			for (auto &cond : join_expressions) {
				if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
					continue;
				}

				auto comparison =
				    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
				if (filter_set.find(*comparison) != filter_set.end()) {
					continue;
				}


			}
		}
	}
};

class PredicateTransferOptimizer {
public:
	explicit PredicateTransferOptimizer(ClientContext &context) : context(context), dag_manager(context) {
	}

	unique_ptr<LogicalOperator> PreOptimize(unique_ptr<LogicalOperator> plan,
	                                        optional_ptr<RelationStats> stats = nullptr);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats = nullptr);
	unique_ptr<LogicalOperator> InsertCreateBFOperator(unique_ptr<LogicalOperator> plan);
	unique_ptr<LogicalOperator> InsertCreateBFOperator_d(unique_ptr<LogicalOperator> plan);

private:
	ClientContext &context;
	DAGManager dag_manager;

	std::unordered_map<void *, unique_ptr<LogicalOperator>> replace_map_forward;
	std::unordered_map<void *, unique_ptr<LogicalOperator>> replace_map_backward;
	static std::unordered_map<std::string, int> table_exists;

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
};
} // namespace duckdb