//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/scan/physical_dummy_scan.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
  public:
	PhysicalDummyScan(std::vector<TypeId> types) : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN, types) {
	}

	// we can hard-code some data into this scan if req
	DataChunk chunk;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};
} // namespace duckdb
