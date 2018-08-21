
#include "execution/operator/physical_table_scan.hpp"

using namespace duckdb;
using namespace std;

vector<TypeId> PhysicalTableScan::GetTypes() {
	vector<TypeId> types;
	for (auto &column_id : column_ids) {
		types.push_back(table->columns[column_id]->column.type);
	}
	return types;
}

void PhysicalTableScan::GetChunk(DataChunk &chunk,
                                 PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalTableScanOperatorState *>(state_);
	chunk.Reset();

	if (column_ids.size() == 0)
		return;

	for (size_t i = 0; i < column_ids.size(); i++) {
		auto *column = table->columns[column_ids[i]].get();
		if (state->current_offset >= column->data.size())
			return;
		auto &v = column->data[state->current_offset];
		chunk.data[i].Reference(*v);
	}
	chunk.count = chunk.data[0].count;
	state->current_offset++;

	chunk.Verify();
}

unique_ptr<PhysicalOperatorState>
PhysicalTableScan::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalTableScanOperatorState>(0, parent_executor);
}
