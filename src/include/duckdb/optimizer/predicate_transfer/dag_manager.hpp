#pragma once

#include "duckdb/optimizer/predicate_transfer/nodes_manager.hpp"
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

class DAGManager {
public:
	explicit DAGManager(ClientContext &context) : binding_manager(context), context(context) {
	}

	bool Build(LogicalOperator &op);

	vector<LogicalOperator *> &GetExecutionOrder();

	void AddBF(idx_t create_bf, const shared_ptr<BlockedBloomFilter> &use_bf, bool reverse);

	NodeBindingManager binding_manager;
	ClientContext &context;
	GraphNodes nodes;

private:
	void ExtractEdges(vector<reference<LogicalOperator>> &join_operators);

	void LargestRoot(vector<LogicalOperator *> &sorted_nodes);

	void CreateDAG();

	pair<int, int> FindEdge(unordered_set<int> &constructed_set, unordered_set<int> &unconstructed_set);

private:
	struct PairHash {
		std::size_t operator()(const pair<int, int> &m) const {
			std::hash<int> hashVal;
			return hashVal(m.first) ^ hashVal(m.second);
		}
	};

	struct PairEqual {
		bool operator()(const pair<int, int> &lhs, const pair<int, int> &rhs) const {
			return lhs.first == rhs.first && lhs.second == rhs.second;
		}
	};

	// (small table, large table), (large table, small table) are both valid
	unordered_map<pair<int, int>, vector<shared_ptr<EdgeInfo>>, PairHash, PairEqual> edges;
	vector<shared_ptr<EdgeInfo>> selected_edges;
	vector<LogicalOperator *> TransferOrder;
};
} // namespace duckdb