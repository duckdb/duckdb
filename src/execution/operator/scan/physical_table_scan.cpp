#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

class PhysicalTableScanOperatorState : public PhysicalOperatorState {
public:
	PhysicalTableScanOperatorState() : PhysicalOperatorState(nullptr), initialized(false) {
	}

	//! Whether or not the scan has been initialized
	bool initialized;
	//! The current position in the scan
	TableScanState scan_offset;
};

void PhysicalTableScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalTableScanOperatorState *>(state_);
	if (column_ids.size() == 0) {
		return;
	}
	auto &transaction = Transaction::GetTransaction(context);
	if (!state->initialized) {
		table.InitializeScan(transaction, state->scan_offset, column_ids);
		state->initialized = true;
	}

	table.Scan(transaction, chunk, state->scan_offset);
}

string PhysicalTableScan::ExtraRenderInformation() const {
	return tableref.name;
}

unique_ptr<PhysicalOperatorState> PhysicalTableScan::GetOperatorState() {
	return make_unique<PhysicalTableScanOperatorState>();
}
