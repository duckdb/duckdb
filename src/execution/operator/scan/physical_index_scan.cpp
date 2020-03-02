#include "duckdb/execution/operator/scan/physical_index_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

class PhysicalIndexScanOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexScanOperatorState() : PhysicalOperatorState(nullptr), initialized(false) {
	}

	bool initialized;
	TableIndexScanState scan_state;
};

void PhysicalIndexScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexScanOperatorState *>(state_);
	if (column_ids.size() == 0) {
		return;
	}

	auto &transaction = Transaction::GetTransaction(context);
	if (!state->initialized) {
		// initialize the scan state of the index
		if (low_index && high_index) {
			// two predicates
			table.InitializeIndexScan(transaction, state->scan_state, index, low_value, low_expression_type, high_value,
			                          high_expression_type, column_ids);
		} else {
			// single predicate
			Value value;
			ExpressionType type;
			if (low_index) {
				// > or >=
				value = low_value;
				type = low_expression_type;
			} else if (high_index) {
				// < or <=
				value = high_value;
				type = high_expression_type;
			} else {
				// equality
				assert(equal_index);
				value = equal_value;
				type = ExpressionType::COMPARE_EQUAL;
			}
			table.InitializeIndexScan(transaction, state->scan_state, index, value, type, column_ids);
		}
		state->initialized = true;
	}
	// scan the index
	table.IndexScan(transaction, chunk, state->scan_state);
}

string PhysicalIndexScan::ExtraRenderInformation() const {
	return tableref.name + "[" + low_value.ToString() + "]";
}

unique_ptr<PhysicalOperatorState> PhysicalIndexScan::GetOperatorState() {
	return make_unique<PhysicalIndexScanOperatorState>();
}
