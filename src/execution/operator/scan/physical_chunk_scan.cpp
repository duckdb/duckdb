#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"

namespace duckdb {

class PhysicalChunkScanState : public PhysicalOperatorState {
public:
	explicit PhysicalChunkScanState(PhysicalOperator &op) : PhysicalOperatorState(op, nullptr), chunk_index(0) {
	}

	//! The current position in the scan
	idx_t chunk_index;
};

void PhysicalChunkScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                         PhysicalOperatorState *state_p) const {
	auto state = (PhysicalChunkScanState *)state_p;
	D_ASSERT(collection);
	if (collection->Count() == 0) {
		return;
	}
	D_ASSERT(chunk.GetTypes() == collection->Types());
	if (state->chunk_index >= collection->ChunkCount()) {
		return;
	}
	auto &collection_chunk = collection->GetChunk(state->chunk_index);
	chunk.Reference(collection_chunk);
	state->chunk_index++;
}

unique_ptr<PhysicalOperatorState> PhysicalChunkScan::GetOperatorState() {
	return make_unique<PhysicalChunkScanState>(*this);
}

} // namespace duckdb
