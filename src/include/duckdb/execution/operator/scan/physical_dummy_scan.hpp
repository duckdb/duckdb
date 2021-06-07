//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
public:
	explicit PhysicalDummyScan(vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN, move(types), estimated_cardinality) {
	}

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
};
} // namespace duckdb
