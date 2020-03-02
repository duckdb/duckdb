//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {
class PhysicalRecursiveCTE : public PhysicalOperator {
public:
	PhysicalRecursiveCTE(LogicalOperator &op, bool union_all, unique_ptr<PhysicalOperator> top,
	                     unique_ptr<PhysicalOperator> bottom);

	bool union_all;
	std::shared_ptr<ChunkCollection> working_table;
	ChunkCollection intermediate_table;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

private:
	//! Probe Hash Table and eliminate duplicate rows
	idx_t ProbeHT(DataChunk &chunk, PhysicalOperatorState *state);
};

}; // namespace duckdb
