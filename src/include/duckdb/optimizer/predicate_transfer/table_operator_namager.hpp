#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {
class TableOperatorManager {
public:
	explicit TableOperatorManager(ClientContext &context) : context(context) {
	}

	ClientContext &context;

	vector<LogicalOperator *> sorted_table_operators;
	unordered_map<idx_t, LogicalOperator *> table_operators;

public:
	vector<reference<LogicalOperator>> ExtractOperators(LogicalOperator &plan);
	void SortTableOperators();

	LogicalOperator *GetTableOperator(idx_t table_idx);
	idx_t GetTableOperatorOrder(const LogicalOperator *node);
	ColumnBinding GetRenaming(ColumnBinding col_binding);

	static idx_t GetScalarTableIndex(LogicalOperator *op);
	static bool OperatorNeedsRelation(LogicalOperatorType op_type);

private:
	void AddTableOperator(LogicalOperator *op);
	void ExtractOperatorsInternal(LogicalOperator &plan, vector<reference<LogicalOperator>> &joins,
	                              bool can_add_mark_join = false);

	struct HashFunc {
		size_t operator()(const ColumnBinding &key) const {
			return std::hash<uint64_t> {}(key.table_index) ^ (std::hash<uint64_t> {}(key.column_index) << 1);
		}
	};
	unordered_map<ColumnBinding, ColumnBinding, HashFunc> rename_col_bindings;
};
} // namespace duckdb
