//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_prune_columns.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalPruneColumns represents a node that prunes extra columns from its
//! children
class LogicalPruneColumns : public LogicalOperator {
public:
	LogicalPruneColumns(index_t column_limit)
	    : LogicalOperator(LogicalOperatorType::PRUNE_COLUMNS), column_limit(column_limit) {
	}

	index_t column_limit;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
