//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/scan/physical_chunk_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalChunkCollectionScan scans a Chunk Collection
class PhysicalChunkScan : public PhysicalOperator {
public:
	PhysicalChunkScan(vector<TypeId> types)
	    : PhysicalOperator(PhysicalOperatorType::CHUNK_SCAN, types), collection(nullptr) {
	}

	// the chunk collection to scan
	ChunkCollection *collection;
	//! Owned chunk collection, if any
	unique_ptr<ChunkCollection> owned_collection;

	void AcceptExpressions(SQLNodeVisitor *v) override{};

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalChunkScanState : public PhysicalOperatorState {
public:
	PhysicalChunkScanState() : PhysicalOperatorState(nullptr), chunk_index(0) {
	}

	//! The current position in the scan
	size_t chunk_index;
};
} // namespace duckdb
