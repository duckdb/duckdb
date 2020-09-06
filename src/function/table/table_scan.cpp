#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/parallel/task_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class TableScanTaskInfo : public OperatorTaskInfo {
public:
	TableScanState state;
};

struct TableScanOperatorData : public FunctionOperatorData {
	//! The current position in the scan
	TableScanState scan_state;
	vector<column_t> column_ids;
	unordered_map<idx_t, vector<TableFilter>> table_filters;
};

unique_ptr<FunctionOperatorData> table_scan_init(ClientContext &context, const FunctionData *bind_data_,
	OperatorTaskInfo *task_info, vector<column_t> &column_ids, unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto result = make_unique<TableScanOperatorData>();
	auto &transaction = Transaction::GetTransaction(context);
	auto &bind_data = (const TableScanBindData &) *bind_data_;
	result->column_ids = column_ids;
	result->table_filters = table_filters;
	if (task_info) {
		auto &info = (TableScanTaskInfo &) *task_info;
		result->scan_state = move(info.state);
	} else {
		bind_data.table->storage->InitializeScan(transaction, result->scan_state, result->column_ids, &result->table_filters);
	}
	return move(result);
}

void table_scan_function(ClientContext &context, const FunctionData *bind_data_, FunctionOperatorData *operator_state, DataChunk &output) {
 	auto &bind_data = (const TableScanBindData &) *bind_data_;
	auto &state = (TableScanOperatorData&) *operator_state;
	auto &transaction = Transaction::GetTransaction(context);
	bind_data.table->storage->Scan(transaction, output, state.scan_state, state.column_ids, state.table_filters);
}

void table_scan_parallel(ClientContext &context, const FunctionData *bind_data_, vector<column_t> &column_ids, unordered_map<idx_t, vector<TableFilter>> &table_filters, std::function<void(unique_ptr<OperatorTaskInfo>)> callback) {
 	auto &bind_data = (const TableScanBindData &) *bind_data_;
	bind_data.table->storage->InitializeParallelScan(context, column_ids, &table_filters, [&](TableScanState state) {
		auto task = make_unique<TableScanTaskInfo>();
		task->state = move(state);
		callback(move(task));
	});
}

void table_scan_dependency(unordered_set<CatalogEntry*> &entries, const FunctionData *bind_data_) {
 	auto &bind_data = (const TableScanBindData &) *bind_data_;
	entries.insert(bind_data.table);
}

idx_t table_scan_cardinality(const FunctionData *bind_data_) {
	auto &bind_data = (const TableScanBindData &) *bind_data_;
	return bind_data.table->storage->info->cardinality;
}

string table_scan_to_string(const FunctionData *bind_data_) {
	auto &bind_data = (const TableScanBindData &) *bind_data_;
	string result = "SEQ_SCAN(" + bind_data.table->name + ")";
	return result;
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, table_scan_function);
	scan_function.init = table_scan_init;
	scan_function.parallel_tasks = table_scan_parallel;
	scan_function.dependency = table_scan_dependency;
	scan_function.cardinality = table_scan_cardinality;
	scan_function.to_string = table_scan_to_string;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	return scan_function;
}

}