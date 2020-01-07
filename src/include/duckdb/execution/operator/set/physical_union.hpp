//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class PhysicalUnion : public PhysicalOperator {
public:
	PhysicalUnion(LogicalOperator &op, unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom);

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

}; // namespace duckdb
