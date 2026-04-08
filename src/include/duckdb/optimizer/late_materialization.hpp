//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/late_materialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "duckdb/common/constants.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class LogicalOperator;
class LogicalGet;
class LogicalLimit;
class Optimizer;
struct ProjectionIndex;
struct TableIndex;

//! Transform
class LateMaterialization : public BaseColumnPruner {
public:
	explicit LateMaterialization(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	bool TryLateMaterialization(unique_ptr<LogicalOperator> &op);

	unique_ptr<LogicalGet> ConstructLHS(LogicalGet &get);
	vector<ColumnBinding> ConstructRHS(unique_ptr<LogicalOperator> &op);
	vector<idx_t> GetOrInsertRowIds(LogicalGet &get);

	void ReplaceTopLevelTableIndex(LogicalOperator &op, TableIndex new_index);
	void ReplaceTableReferences(unique_ptr<Expression> &expr, TableIndex new_table_index);
	unique_ptr<Expression> GetExpression(LogicalOperator &op, ProjectionIndex column_index);
	void ReplaceExpressionReferences(LogicalOperator &next_op, unique_ptr<Expression> &expr);
	bool OptimizeLargeLimit(LogicalLimit &limit, idx_t limit_val, bool has_offset);

private:
	Optimizer &optimizer;
	//! The max row count for which we will consider late materialization
	idx_t max_row_count;
	//! The row-id column ids
	vector<column_t> row_id_column_ids;
	//! The row-id columns
	vector<TableColumn> row_id_columns;
};

} // namespace duckdb
