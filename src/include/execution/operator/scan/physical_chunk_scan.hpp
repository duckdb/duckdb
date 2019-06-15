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
	PhysicalChunkScan(vector<TypeId> types, PhysicalOperatorType op_type)
	    : PhysicalOperator(op_type, types), collection(nullptr) {
	}

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

public:
	// the chunk collection to scan
	ChunkCollection *collection;
	//! Owned chunk collection, if any
	unique_ptr<ChunkCollection> owned_collection;
};

class PhysicalChunkScanState : public PhysicalOperatorState {
public:
	PhysicalChunkScanState() : PhysicalOperatorState(nullptr), chunk_index(0) {
	}

	//! The current position in the scan
	index_t chunk_index;
};
} // namespace duckdb
