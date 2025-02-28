#pragma once

#include "duckdb/optimizer/predicate_transfer/table_operator_namager.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class EdgeInfo {
public:
	EdgeInfo(unique_ptr<Expression> filter, LogicalOperator &bigger, LogicalOperator &smaller)
	    : condition(std::move(filter)), bigger_table(bigger), smaller_table(smaller) {
	}

	unique_ptr<Expression> condition;

	LogicalOperator &bigger_table;
	LogicalOperator &smaller_table;
	bool protect_bigger_side = false;
	bool protect_smaller_side = false;
};

class TransferGraphManager {
public:
	explicit TransferGraphManager(ClientContext &context) : context(context), table_operator_manager(context) {
	}

	ClientContext &context;
	TableOperatorManager table_operator_manager;
	TransferGraph transfer_graph;
	vector<LogicalOperator *> transfer_order;

public:
	bool Build(LogicalOperator &op);
	void AddBF(idx_t create_bf, const shared_ptr<BlockedBloomFilter> &use_bf, bool reverse);

private:
	void ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators);
	void LargestRoot(vector<LogicalOperator *> &sorted_nodes);
	void CreatePredicateTransferGraph();

	pair<int, int> FindEdge(const unordered_set<int> &constructed_set, const unordered_set<int> &unconstructed_set);

private:
	struct PairHash {
		std::size_t operator()(const std::pair<int, int> &p) const {
			return std::hash<int> {}(p.first) ^ (std::hash<int> {}(p.second) << 1);
		}
	};
	unordered_map<std::pair<int, int>, vector<shared_ptr<EdgeInfo>>, PairHash> edges_info;
	vector<shared_ptr<EdgeInfo>> selected_edges;
};
} // namespace duckdb