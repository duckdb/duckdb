#include "execution/operator/scan/physical_index_scan.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalIndexScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexScanOperatorState *>(state_);
	if (column_ids.size() == 0)
		return;

	if (!state->scan_state) {
		// initialize the scan state of the index
		// We have a query with two predicates
		if (low_index && high_index) {
			state->scan_state =
			    index.InitializeScanTwoPredicates(context.ActiveTransaction(), column_ids, low_value,
			                                      low_expression_type, high_value, high_expression_type);
		}
		// Our query has only one predicate
		else {
			if (low_index)
				state->scan_state = index.InitializeScanSinglePredicate(context.ActiveTransaction(), column_ids,
				                                                        low_value, low_expression_type);
			else if (high_index)
				state->scan_state = index.InitializeScanSinglePredicate(context.ActiveTransaction(), column_ids,
				                                                        high_value, high_expression_type);
			else if (equal_index)
				state->scan_state = index.InitializeScanSinglePredicate(context.ActiveTransaction(), column_ids,
				                                                        equal_value, ExpressionType::COMPARE_EQUAL);
		}
	}

	//! Continue the scan of the index
	index.Scan(context.ActiveTransaction(), state->scan_state.get(), chunk);
}

string PhysicalIndexScan::ExtraRenderInformation() const {
	return tableref.name + "[" + low_value.ToString() + "]";
}

unique_ptr<PhysicalOperatorState> PhysicalIndexScan::GetOperatorState() {
	return make_unique<PhysicalIndexScanOperatorState>();
}
