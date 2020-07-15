#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include <utility>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include "duckdb/parallel/task_context.hpp"

using namespace std;

namespace duckdb {

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
	TableScanState scan_state;
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

class TableScanTaskInfo : public OperatorTaskInfo {
public:
	TableScanState state;
};

void PhysicalTableScan::ParallelScanInfo(ClientContext &context, std::function<void(unique_ptr<OperatorTaskInfo>)> callback) {
	// generate parallel scans
	auto task = make_unique<TableScanTaskInfo>();
	table.InitializeScan(Transaction::GetTransaction(context), task->state, column_ids, &table_filters);
	callback(move(task));
}

void PhysicalTableScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalTableScanOperatorState *>(state_);
	if (column_ids.empty()) {
		return;
	}
	auto &transaction = Transaction::GetTransaction(context.client);
	if (!state->initialized) {
		auto &task = context.task;
		auto task_info = task.task_info.find(this);
		if (task_info != task.task_info.end()) {
			// task specific limitations: scan the part indicated by the task
			auto &info = (TableScanTaskInfo &) *task_info->second;
			state->scan_state = move(info.state);
		} else {
			// no task specific limitations for the scan: scan the entire table
			table.InitializeScan(transaction, state->scan_state, column_ids, &table_filters);
		}
		state->initialized = true;
	}
	table.Scan(transaction, chunk, state->scan_state, column_ids, table_filters);
}

string PhysicalTableScan::ExtraRenderInformation() const {
	if (expression) {
		return tableref.name + " " + expression->ToString();
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

} // namespace duckdb
