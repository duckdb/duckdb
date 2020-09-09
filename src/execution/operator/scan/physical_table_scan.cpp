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
	PhysicalTableScanOperatorState(PhysicalOperator &op) : PhysicalOperatorState(op, nullptr), initialized(false) {
	}

	unique_ptr<FunctionOperatorData> operator_data;
	//! Whether or not the scan has been initialized
	bool initialized;
};

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_,
                                     unique_ptr<FunctionData> bind_data_, vector<column_t> column_ids,
                                     unordered_map<idx_t, vector<TableFilter>> table_filters)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, move(types)), function(move(function_)),
      bind_data(move(bind_data_)), column_ids(move(column_ids)), table_filters(move(table_filters)) {
}

void PhysicalTableScan::ParallelScanInfo(ClientContext &context,
                                         std::function<void(unique_ptr<OperatorTaskInfo>)> callback) {
	// generate parallel scans
	if (function.parallel_tasks) {
		function.parallel_tasks(context, bind_data.get(), column_ids, table_filters, callback);
	}
}

void PhysicalTableScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto &state = (PhysicalTableScanOperatorState &)*state_;
	if (column_ids.empty()) {
		return;
	}
	if (!state.initialized) {
		if (function.init) {
			auto &task = context.task;
			auto task_info = task.task_info.find(this);
			if (task_info != task.task_info.end()) {
				// task specific limitations: pass the task information to the init function
				state.operator_data =
				    function.init(context.client, bind_data.get(), task_info->second.get(), column_ids, table_filters);
			} else {
				// no task specific limitations
				state.operator_data =
				    function.init(context.client, bind_data.get(), nullptr, column_ids, table_filters);
			}
		}
		state.initialized = true;
	}
	function.function(context.client, bind_data.get(), state.operator_data.get(), chunk);
	if (chunk.size() == 0 && function.cleanup) {
		function.cleanup(context.client, bind_data.get(), state.operator_data.get());
	}
}

string PhysicalTableScan::ToString(idx_t depth) const {
	if (function.to_string) {
		return string(depth * 4, ' ') + function.to_string(bind_data.get());
	}
	return PhysicalOperator::ToString(depth);
}

unique_ptr<PhysicalOperatorState> PhysicalTableScan::GetOperatorState() {
	return make_unique<PhysicalTableScanOperatorState>(*this);
}

} // namespace duckdb
