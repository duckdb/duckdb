#pragma once

#include "duckdb/optimizer/predicate_transfer/nodes_manager.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DAGEdgeInfo {
public:
	DAGEdgeInfo(unique_ptr<Expression> filter, LogicalOperator &large, LogicalOperator &small)
	    : filter(std::move(filter)), large_(large), small_(small) {
	}

	unique_ptr<Expression> filter;
	LogicalOperator &large_;
	LogicalOperator &small_;
	bool large_protect = false;
	bool small_protect = false;
};

class DAGManager {
public:
	explicit DAGManager(ClientContext &context) : nodes_manager(context), context(context) {
	}

	bool Build(LogicalOperator &op);

	vector<LogicalOperator *> &GetExecutionOrder();

	void Add(idx_t create_table, shared_ptr<BlockedBloomFilter> use_bf, bool reverse);

	NodesManager nodes_manager;
	ClientContext &context;
	GraphNodes nodes;

private:
	void ExtractEdges(LogicalOperator &op, vector<reference<LogicalOperator>> &join_operators);

	void LargestRoot(vector<LogicalOperator *> &sorted_nodes);

	void CreateDAG();

	pair<int, int> FindEdge(unordered_set<int> &constructed_set, unordered_set<int> &unconstructed_set);

	vector<GraphNode *> GetNeighbors(idx_t node_id);

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
	unordered_map<pair<int, int>, vector<shared_ptr<DAGEdgeInfo>>, PairHash, PairEqual> edges;
	vector<shared_ptr<DAGEdgeInfo>> selected_edges;
	vector<LogicalOperator *> TransferOrder;
};
} // namespace duckdb