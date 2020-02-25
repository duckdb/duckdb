//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_prune_columns.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalPruneColumns prunes (removes) columns from its input
class PhysicalPruneColumns : public PhysicalOperator {
public:
	PhysicalPruneColumns(LogicalOperator &op, idx_t column_limit)
	    : PhysicalOperator(PhysicalOperatorType::PRUNE_COLUMNS, op.types), column_limit(column_limit) {
	}

	idx_t column_limit;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};
} // namespace duckdb
