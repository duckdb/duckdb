#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {
class NodesManager {
public:
	explicit NodesManager(ClientContext &context) : context(context) {
	}

	void Reset();

	idx_t NumNodes();

	void AddNode(LogicalOperator *op);

	void SortNodes();

	LogicalOperator *GetNode(idx_t table_binding) {
		auto itr = nodes.find(table_binding);
		if (itr == nodes.end()) {
			return nullptr;
		}

		return itr->second;
	}

	unordered_map<idx_t, LogicalOperator *> &GetNodes() {
		return nodes;
	}

	void EraseNode(idx_t key);

	void DuplicateNodes() {
		duplicate_nodes = nodes;
	}

	void RecoverNodes() {
		nodes = duplicate_nodes;
	}

	vector<LogicalOperator *> &getSortedNodes() {
		return sort_nodes;
	}

	//! Extract All the vertex nodes
	void ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &joins);

	ColumnBinding FindRename(ColumnBinding col);

	static idx_t GetScalarTableIndex(LogicalOperator *op);

private:
	struct HashFunc {
		size_t operator()(const ColumnBinding &key) const {
			return std::hash<uint64_t>()(key.table_index) ^ std::hash<uint64_t>()(key.column_index);
		}
	};

	struct CmpFunc {
		bool operator()(const ColumnBinding &a, const ColumnBinding &b) const {
			return (a.table_index == b.table_index) && (a.column_index == b.column_index);
		}
	};

private:
	ClientContext &context;

	bool can_add_mark = true;

	unordered_map<idx_t, LogicalOperator *> nodes;
	unordered_map<idx_t, LogicalOperator *> duplicate_nodes;
	vector<LogicalOperator *> sort_nodes;
	unordered_map<ColumnBinding, ColumnBinding, HashFunc, CmpFunc> rename_cols;
};
} // namespace duckdb