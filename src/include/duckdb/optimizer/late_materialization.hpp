//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/late_materialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/common/table_column.hpp"

namespace duckdb {
class LogicalOperator;
class LogicalGet;
class LogicalLimit;
class Optimizer;

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

	void ReplaceTopLevelTableIndex(LogicalOperator &op, idx_t new_index);
	void ReplaceTableReferences(unique_ptr<Expression> &expr, idx_t new_table_index);
	unique_ptr<Expression> GetExpression(LogicalOperator &op, idx_t column_index);
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
