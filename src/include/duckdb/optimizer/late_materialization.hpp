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
class Optimizer;

//! Transform
class LateMaterialization : public BaseColumnPruner {
public:
	explicit LateMaterialization(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	bool TryLateMaterialization(unique_ptr<LogicalOperator> &op);

	ColumnBinding ConstructRHS(unique_ptr<LogicalOperator> &op);

private:
	Optimizer &optimizer;
	//! The max row count for which we will consider late materialization
	idx_t max_row_count = 50;
};

} // namespace duckdb
