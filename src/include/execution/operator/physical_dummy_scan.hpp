//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// execution/operator/physical_dummy_scan.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
  public:
	PhysicalDummyScan() : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN) {
	}

	// we can hard-code some data into this scan if req
	DataChunk chunk;

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};
} // namespace duckdb
