#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include <utility>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalTableScanOperatorState : public PhysicalOperatorState {
public:
	PhysicalTableScanOperatorState(Expression &expr)
	    : PhysicalOperatorState(nullptr), initialized(false), executor(expr) {
	}
	PhysicalTableScanOperatorState() : PhysicalOperatorState(nullptr), initialized(false) {
	}
	//! Whether or not the scan has been initialized
	bool initialized;
	//! The current position in the scan
	TableScanState scan_offset;
	//! Execute filters inside the table
	ExpressionExecutor executor;
};

PhysicalTableScan::PhysicalTableScan(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table,
                                     vector<column_t> column_ids, vector<unique_ptr<Expression>> filter,
                                     unordered_map<idx_t, vector<TableFilter>> table_filters)
    : PhysicalOperator(PhysicalOperatorType::SEQ_SCAN, op.types), tableref(tableref), table(table),
      column_ids(move(column_ids)), table_filters(move(table_filters)) {
	if (filter.size() > 1) {
		//! create a big AND out of the expressions
		auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : filter) {
			conjunction->children.push_back(move(expr));
		}
		expression = move(conjunction);
	} else if (filter.size() == 1) {
		expression = move(filter[0]);
	}
}
void PhysicalTableScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalTableScanOperatorState *>(state_);
	bool applyFilters = false;
	if (column_ids.empty()) {
		return;
	}
	auto &transaction = Transaction::GetTransaction(context);
	if (!state->initialized) {
		table.InitializeScan(transaction, state->scan_offset, column_ids);
		state->initialized = true;
	}

	do {
        chunk.Reset();
        //! In case of updates or scans in the transaction itself we fall back to executing the filter after fetching the
        //! data
        applyFilters = table.Scan(transaction, chunk, state->scan_offset, table_filters);
        if (applyFilters && expression) {
            SelectionVector sel(STANDARD_VECTOR_SIZE);
            idx_t initial_count = chunk.size();
            idx_t result_count = state->executor.SelectExpression(chunk, sel);
            if (result_count == initial_count) {
                //! Nothing was filtered: skip adding any selection vectors
                return;
            }
            chunk.Slice(sel, result_count);
        }
    } while (expression &&  chunk.size() == 0);
}

string PhysicalTableScan::ExtraRenderInformation() const {
	if (expression) {
		return tableref.name + " " + expression->GetName();
	} else {
		return tableref.name;
	}
}

unique_ptr<PhysicalOperatorState> PhysicalTableScan::GetOperatorState() {
	if (expression) {
		return make_unique<PhysicalTableScanOperatorState>(*expression);
	} else {
		return make_unique<PhysicalTableScanOperatorState>();
	}
}
