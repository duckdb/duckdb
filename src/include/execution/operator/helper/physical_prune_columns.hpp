//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/helper/physical_prune_columns.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalPruneColumns prunes (removes) columns from its input
class PhysicalPruneColumns : public PhysicalOperator {
public:
	PhysicalPruneColumns(LogicalOperator &op, count_t column_limit)
	    : PhysicalOperator(PhysicalOperatorType::PRUNE_COLUMNS, op.types), column_limit(column_limit) {
	}

	count_t column_limit;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};
} // namespace duckdb
