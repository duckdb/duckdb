
#include "execution/operator/seqscan.hpp"

using namespace duckdb;
using namespace std;

void PhysicalSeqScan::InitializeChunk(DataChunk &chunk) {
	// just copy the chunk data of the child
	vector<TypeId> types;
	for (auto &column_id : column_ids) {
		types.push_back(table->columns[column_id]->type);
	}
	chunk.Initialize(types);
}

void PhysicalSeqScan::GetChunk(DataChunk &chunk,
                               PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSeqScanOperatorState *>(state_);
	chunk.Reset();

	if (state->current_offset >= table->size)
		return;

	size_t element_count =
	    min(chunk.maximum_size, table->size - state->current_offset);

	for (size_t i = 0; i < column_ids.size(); i++) {
		auto *column = table->columns[column_ids[i]].get();
		char *column_data = (char *)column->data;
		size_t element_size = GetTypeIdSize(column->type);
		chunk.data[i]->data =
		    column_data + element_size * state->current_offset;
		chunk.data[i]->owns_data = false;
		chunk.data[i]->count = chunk.count;
	}
	chunk.count = element_count;
	state->current_offset += element_count;
}

unique_ptr<PhysicalOperatorState> PhysicalSeqScan::GetOperatorState() {
	return make_unique<PhysicalSeqScanOperatorState>(0);
}
