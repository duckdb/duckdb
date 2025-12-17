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
class LogicalComparisonJoin;
class Optimizer;

//! Transform
class LateMaterialization : public BaseColumnPruner {
public:
	explicit LateMaterialization(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	bool TryLateMaterialization(unique_ptr<LogicalOperator> &op);
	//! Try late materialization for semi-joins with expensive columns on probe side
	bool TryLateMaterializationSemiJoin(unique_ptr<LogicalOperator> &op);

	unique_ptr<LogicalGet> ConstructLHS(LogicalGet &get);
	vector<ColumnBinding> ConstructRHS(unique_ptr<LogicalOperator> &op);
	vector<idx_t> GetOrInsertRowIds(LogicalGet &get);

	void ReplaceTopLevelTableIndex(LogicalOperator &op, idx_t new_index);
	void ReplaceTableReferences(unique_ptr<Expression> &expr, idx_t new_table_index);
	unique_ptr<Expression> GetExpression(LogicalOperator &op, idx_t column_index);
	void ReplaceExpressionReferences(LogicalOperator &next_op, unique_ptr<Expression> &expr);
	bool OptimizeLargeLimit(LogicalLimit &limit, idx_t limit_val, bool has_offset);

	//! Check if a column type is considered "expensive" to project
	static bool IsExpensiveColumnType(const LogicalType &type);
	//! Find the LogicalGet in the probe side of a join, traversing through filters/projections
	LogicalGet *FindProbeGet(LogicalOperator &op, vector<reference<LogicalOperator>> &path);
	//! Get column bindings that are used in join conditions
	column_binding_set_t GetJoinConditionBindings(LogicalComparisonJoin &join);
	//! Check if the probe side has expensive columns not needed for join
	bool HasExpensiveNonJoinColumns(LogicalGet &get, const column_binding_set_t &join_bindings,
	                                vector<idx_t> &expensive_column_indices);

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
