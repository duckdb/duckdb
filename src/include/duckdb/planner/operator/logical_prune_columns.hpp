//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_prune_columns.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalPruneColumns represents a node that prunes extra columns from its
//! children
class LogicalPruneColumns : public LogicalOperator {
public:
	LogicalPruneColumns(idx_t column_limit)
	    : LogicalOperator(LogicalOperatorType::PRUNE_COLUMNS), column_limit(column_limit) {
	}

	idx_t column_limit;

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
