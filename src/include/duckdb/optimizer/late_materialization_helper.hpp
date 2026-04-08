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
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Binder;

struct LateMaterializationHelper {
	static unique_ptr<LogicalGet> CreateLHSGet(const LogicalGet &rhs, Binder &binder);
	static vector<ProjectionIndex> GetOrInsertRowIds(LogicalGet &get, const vector<column_t> &row_id_column_ids,
	                                                 const vector<TableColumn> &row_id_columns);
};

} // namespace duckdb
