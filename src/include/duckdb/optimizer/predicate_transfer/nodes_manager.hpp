#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {
class NodeBindingManager {
public:
	explicit NodeBindingManager(ClientContext &context) : context(context) {
	}

	ClientContext &context;

	vector<LogicalOperator *> sorted_nodes;
	unordered_map<idx_t, LogicalOperator *> nodes;

public:
	//! Extract All the vertex nodes
	vector<reference<LogicalOperator>> ExtractNodesAndJoins(LogicalOperator &plan);

	void SortNodes();
	LogicalOperator *GetNode(idx_t table_idx);
	idx_t GetNodeOrder(const LogicalOperator *node);

	ColumnBinding FindRename(ColumnBinding col_binding);
	static idx_t GetScalarTableIndex(LogicalOperator *op);


	void ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &joins);

private:
	void AddNode(LogicalOperator *op);

	bool can_add_mark = true;

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
	unordered_map<ColumnBinding, ColumnBinding, HashFunc, CmpFunc> rename_cols;
};
} // namespace duckdb