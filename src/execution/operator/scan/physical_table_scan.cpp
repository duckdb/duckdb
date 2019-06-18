#include "execution/operator/scan/physical_table_scan.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalTableScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalTableScanOperatorState *>(state_);
	if (column_ids.size() == 0)
		return;

	table.Scan(context.ActiveTransaction(), chunk, column_ids, state->scan_offset);
}

string PhysicalTableScan::ExtraRenderInformation() const {
	return tableref.name;
}

unique_ptr<PhysicalOperatorState> PhysicalTableScan::GetOperatorState() {
	return make_unique<PhysicalTableScanOperatorState>(table);
}
