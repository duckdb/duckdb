
#include "execution/operator/physical_table_scan.hpp"

#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

vector<string> PhysicalTableScan::GetNames() {
	vector<string> names;
	for (auto &column : table.table.columns) {
		names.push_back(column.name);
	}
	return names;
}

vector<TypeId> PhysicalTableScan::GetTypes() {
	return table.GetTypes(column_ids);
}

void PhysicalTableScan::_GetChunk(ClientContext &context, DataChunk &chunk,
                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalTableScanOperatorState *>(state_);
	chunk.Reset();

	if (column_ids.size() == 0)
		return;

	table.Scan(context.ActiveTransaction(), chunk, column_ids,
	           state->scan_offset);
}

unique_ptr<PhysicalOperatorState>
PhysicalTableScan::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalTableScanOperatorState>(table, parent_executor);
}

string PhysicalTableScan::ExtraRenderInformation() {
	return table.table.name;
}
