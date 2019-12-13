//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_delim_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
//! PhysicalDelimJoin represents a join where the LHS will be duplicate eliminated and pushed into a
//! PhysicalChunkCollectionScan in the RHS.
class PhysicalDelimJoin : public PhysicalOperator {
public:
	PhysicalDelimJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> original_join,
	                  vector<PhysicalOperator *> delim_scans);

	unique_ptr<PhysicalOperator> join;
	unique_ptr<PhysicalOperator> distinct;
	ChunkCollection lhs_data;
	ChunkCollection delim_data;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	string ExtraRenderInformation() const override;
};

} // namespace duckdb
