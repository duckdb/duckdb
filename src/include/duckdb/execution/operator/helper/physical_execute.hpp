//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalExecute : public PhysicalOperator {
public:
	PhysicalExecute(PhysicalOperator *plan) : PhysicalOperator(PhysicalOperatorType::EXECUTE, plan->types), plan(plan) {
	}

	PhysicalOperator *plan;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
