#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"

namespace duckdb {

class PhysicalChunkScanState : public GlobalSourceState {
public:
	explicit PhysicalChunkScanState() : chunk_index(0) {
	}

	//! The current position in the scan
	idx_t chunk_index;
};

unique_ptr<GlobalSourceState> PhysicalChunkScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalChunkScanState>();
}

void PhysicalChunkScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                LocalSourceState &lstate) const {
	auto &state = (PhysicalChunkScanState &)gstate;
	D_ASSERT(collection);
	if (collection->Count() == 0) {
		return;
	}
	D_ASSERT(chunk.GetTypes() == collection->Types());
	if (state.chunk_index >= collection->ChunkCount()) {
		return;
	}
	auto &collection_chunk = collection->GetChunk(state.chunk_index);
	chunk.Reference(collection_chunk);
	state.chunk_index++;
}

} // namespace duckdb
