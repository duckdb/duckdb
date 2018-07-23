
#include "execution/operator/seqscan.hpp"

using namespace duckdb;
using namespace std;

void PhysicalSeqScan::InitializeChunk(DataChunk &chunk) {
	// just copy the chunk data of the child
	vector<TypeId> types;
	for(auto& column_id : column_ids) {
		types.push_back(table->columns[column_id]->column.type);
	}
	chunk.Initialize(types);
}

void PhysicalSeqScan::GetChunk(DataChunk &chunk,
                               PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSeqScanOperatorState *>(state_);
	chunk.Reset();

	if (column_ids.size() == 0)
		return;

	for(size_t i = 0; i < column_ids.size(); i++) {
		auto* column = table->columns[column_ids[i]].get();
		if (state->current_offset >= column->data.size())
			return;
		auto& v = column->data[state->current_offset];
		chunk.data[i]->data = v->data;
		chunk.data[i]->owns_data = false;
		chunk.data[i]->count = v->count;
	}
	chunk.count = chunk.data[0]->count;
	state->current_offset++;
}

unique_ptr<PhysicalOperatorState> PhysicalSeqScan::GetOperatorState() {
	return make_unique<PhysicalSeqScanOperatorState>(0);
}
