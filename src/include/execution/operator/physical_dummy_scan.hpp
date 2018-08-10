//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_table_scan.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
  public:
	PhysicalDummyScan() : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN) {}

	virtual void InitializeChunk(DataChunk &chunk) override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};
} // namespace duckdb
