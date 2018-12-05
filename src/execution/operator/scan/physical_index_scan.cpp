
#include "execution/operator/scan/physical_index_scan.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

vector<string> PhysicalIndexScan::GetNames() {
	vector<string> names;
	for (auto &column : tableref.columns) {
		names.push_back(column.name);
	}
	return names;
}

vector<TypeId> PhysicalIndexScan::GetTypes() {
	return table.GetTypes(column_ids);
}

void PhysicalIndexScan::_GetChunk(ClientContext &context, DataChunk &chunk,
                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexScanOperatorState *>(state_);
	chunk.Reset();

	if (column_ids.size() == 0)
		return;

	if (!state->scan_state) {
		// initialize the scan state of the index
		state->scan_state = index.InitializeScan(context.ActiveTransaction(),
		                                         column_ids, expression.get(),expression_type);
	}

	//! Continue the scan of the index
	index.Scan(context.ActiveTransaction(), state->scan_state.get(), chunk);
}

string PhysicalIndexScan::ExtraRenderInformation() {
	return tableref.name + "[" + expression->ToString() + "]";
}

unique_ptr<PhysicalOperatorState>
PhysicalIndexScan::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalIndexScanOperatorState>(parent_executor);
}
