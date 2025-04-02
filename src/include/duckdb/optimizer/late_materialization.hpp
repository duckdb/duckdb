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
	ColumnBinding ConstructRHS(unique_ptr<LogicalOperator> &op);
	idx_t GetOrInsertRowId(LogicalGet &get);

	void ReplaceTopLevelTableIndex(LogicalOperator &op, idx_t new_index);
	void ReplaceTableReferences(Expression &expr, idx_t new_table_index);
	unique_ptr<Expression> GetExpression(LogicalOperator &op, idx_t column_index);
	void ReplaceExpressionReferences(LogicalOperator &next_op, unique_ptr<Expression> &expr);
	bool OptimizeLargeLimit(LogicalLimit &limit, idx_t limit_val, bool has_offset);

private:
	Optimizer &optimizer;
	//! The max row count for which we will consider late materialization
	idx_t max_row_count;
	//! The type of the row id column
	LogicalType row_id_type;
};

} // namespace duckdb
