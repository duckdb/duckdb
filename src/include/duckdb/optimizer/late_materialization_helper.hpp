//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/late_materialization_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

struct LateMaterializationHelper {
	static unique_ptr<LogicalGet> CreateLHSGet(const LogicalGet &rhs, Binder &binder);
	static vector<idx_t> GetOrInsertRowIds(LogicalGet &get, const vector<column_t> &row_id_column_ids,
	                                       const vector<TableColumn> &row_id_columns);
};

} // namespace duckdb
