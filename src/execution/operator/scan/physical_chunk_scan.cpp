#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"

using namespace duckdb;
using namespace std;

class PhysicalChunkScanState : public PhysicalOperatorState {
public:
	PhysicalChunkScanState() : PhysicalOperatorState(nullptr), chunk_index(0) {
	}

	//! The current position in the scan
	index_t chunk_index;
};

void PhysicalChunkScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = (PhysicalChunkScanState *)state_;
	assert(collection);
	if (collection->count == 0) {
		return;
	}
	assert(chunk.GetTypes() == collection->types);
	if (state->chunk_index >= collection->chunks.size()) {
		return;
	}
	auto &collection_chunk = *collection->chunks[state->chunk_index];
	for (index_t i = 0; i < chunk.column_count(); i++) {
		chunk.data[i].Reference(collection_chunk.data[i]);
	}
	state->chunk_index++;
}

unique_ptr<PhysicalOperatorState> PhysicalChunkScan::GetOperatorState() {
	return make_unique<PhysicalChunkScanState>();
}
