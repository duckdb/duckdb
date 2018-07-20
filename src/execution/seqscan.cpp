
#include "execution/seqscan.hpp"

using namespace duckdb;
using namespace std;

void PhysicalSeqScan::GetChunk(DataChunk& chunk, PhysicalOperatorState* state_) {
	auto state = reinterpret_cast<PhysicalSeqScanOperatorState*>(state_);
	chunk.Reset();

	if (state->current_offset >= table->size) return;

	size_t element_count = min(chunk.maximum_size, table->size - state->current_offset);

	size_t i = 0;
	for(auto& col_id : column_ids) {
		auto* column = table->columns[col_id].get();
		char* column_data = (char*) column->data;
		size_t element_size = GetTypeIdSize(column->type);
		memcpy(chunk.data[i], column_data + element_size * state->current_offset, element_size * element_count);
	}
	chunk.count = element_count;
	state->current_offset += element_count;
}

unique_ptr<PhysicalOperatorState> GetOperatorState() {
	return make_unique<PhysicalSeqScanOperatorState>(0);
}
