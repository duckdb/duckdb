//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_empty_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalEmptyResult : public PhysicalOperator {
public:
	PhysicalEmptyResult(vector<LogicalType> types) : PhysicalOperator(PhysicalOperatorType::EMPTY_RESULT, move(types)) {
	}

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};
} // namespace duckdb
