//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/helper/physical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalExecute : public PhysicalOperator {
public:
	PhysicalExecute(PhysicalOperator *plan) : PhysicalOperator(PhysicalOperatorType::EXECUTE, plan->types), plan(plan) {
	}

	PhysicalOperator *plan;

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	void AcceptExpressions(SQLNodeVisitor *v) override{};
};

} // namespace duckdb
