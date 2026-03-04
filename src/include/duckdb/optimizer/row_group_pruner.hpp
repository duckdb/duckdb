//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/row_group_pruner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {
class LogicalGet;
class LogicalOperator;

class RowGroupPruner {
public:
	explicit RowGroupPruner(ClientContext &context);

	//! Reorder and try to prune row groups in queries with LIMIT or simple aggregates
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	bool TryOptimize(LogicalOperator &op) const;

private:
	ClientContext &context;

private:
	void GetLimitAndOffset(const LogicalLimit &logical_limit, optional_idx &row_limit, optional_idx &row_offset) const;
	optional_ptr<LogicalOrder> FindLogicalOrder(const LogicalLimit &logical_limit) const;
	optional_ptr<LogicalGet> FindLogicalGet(const LogicalOrder &logical_order, ColumnIndex &column_index) const;
	// row_limit, row_offset, primary_order, logical_get, logical_limit
	unique_ptr<RowGroupOrderOptions> CreateRowGroupReordererOptions(optional_idx row_limit, optional_idx row_offset,
	                                                                const BoundOrderByNode &primary_order,
	                                                                const LogicalGet &logical_get,
	                                                                const StorageIndex &storage_index,
	                                                                LogicalLimit &logical_limit) const;
};

} // namespace duckdb
