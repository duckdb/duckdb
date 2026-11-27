#include "duckdb/main/capi_v2/capi_v2_data_source_internal.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/partition_info.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/main/profiler/metrics.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb::capiv2 {

class CV2DataSourceGlobalState final : public GlobalTableFunctionState {
public:
	auto MaxThreads() const -> idx_t override {
		return max_threads;
	}

	CV2UserData handle;
	idx_t max_threads = 1;

	// The declared column each vector of the output chunk stands for, kept here so the exec callback can ask.
	vector<column_t> scan_columns;
};

class CV2DataSourceLocalState final : public LocalTableFunctionState {
public:
	CV2UserData handle;
	//! Whether the caller claims the batches of this thread (see TableFunction::claim_batch)
	bool claimed_externally = false;
	//! Whether this thread has claimed a batch it has not exhausted yet
	bool batch_claimed = false;
	//! What the last invocation of the exec callback declared, and the tasks it added
	DUCKDB_V2_DATA_SOURCE_RESULT result = DUCKDB_V2_DATA_SOURCE_RESULT_IMPLICIT;
	vector<unique_ptr<AsyncTask>> tasks;
	//! Written into by the exec callback when the scan requests no columns, as the function produces its rows through
	//! its first column
	unique_ptr<DataChunk> row_chunk;
	//! The filters of the query the engine applies to what the function produces, as the position of the scan column
	//! each applies to and its state. Only for a function that reads the filters.
	vector<pair<idx_t, unique_ptr<TableFilterState>>> filters;
	//! A column the multi-file scan wants converted before its filters apply: the position of the scan column it
	//! produces and the expression doing the conversion, on the columns that follow it in what the function produced
	struct ConvertedColumn {
		idx_t position;
		unique_ptr<Expression> expression;
		unique_ptr<ExpressionExecutor> executor;
		unique_ptr<DataChunk> input;
	};
	vector<ConvertedColumn> converted_columns;
	//! For each scan column, whether it is one of the converted columns
	vector<bool> converted_position;
	//! What the function produces, in the types of its file, when some columns are converted afterwards
	unique_ptr<DataChunk> file_chunk;

	// Requested partition columns as declared columns, fixed for the lifetime of the local state's pipeline.
	const vector<column_t> &PartitionColumns(const OperatorPartitionInfo &partition_info,
	                                         const vector<column_t> &scan_columns) {
		if (!partition_columns_resolved) {
			for (auto scan_position : partition_info.partition_columns) {
				D_ASSERT(scan_position < scan_columns.size());
				partition_columns.push_back(scan_columns[scan_position]);
			}
			partition_columns_resolved = true;
		}
		D_ASSERT(partition_columns.size() == partition_info.partition_columns.size());
		return partition_columns;
	}

	void CheckPartitionData(const Identifier &function_name, const OperatorPartitionData &data) {
		if (last_batch_index.IsValid()) {
			auto last_index = last_batch_index.GetIndex();
			// The engine would otherwise raise an InternalException, which invalidates the database.
			if (data.batch_index < last_index) {
				throw InvalidInputException(
				    "The partition data callback of table function %s reported batch index %llu after "
				    "previously reporting %llu on the same thread; the batch index must not decrease.",
				    function_name, data.batch_index, last_index);
			}
			// The engine only reads the values when the batch index changes; a changed value would silently be ignored.
			if (data.batch_index == last_index) {
				D_ASSERT(data.partition_data.size() == last_partition_values.size());
				for (idx_t i = 0; i < last_partition_values.size(); i++) {
					if (Value::NotDistinctFrom(data.partition_data[i].min_val, last_partition_values[i])) {
						continue;
					}
					throw InvalidInputException(
					    "The partition data callback of table function %s reported a different partitioning "
					    "column value at index %llu without changing the batch index.",
					    function_name, i);
				}
				return;
			}
		}
		last_batch_index = data.batch_index;
		last_partition_values.clear();
		for (auto &entry : data.partition_data) {
			last_partition_values.push_back(entry.min_val);
		}
	}

private:
	bool partition_columns_resolved = false;
	vector<column_t> partition_columns;
	optional_idx last_batch_index;
	vector<Value> last_partition_values;
};

class CV2TableInitGlobalInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	const vector<column_t> *in_scan_columns = nullptr;

	duckdb_v2_opaque out_global_state = {};
	idx_t out_max_threads = 1;
};

static auto Convert(duckdb_v2_data_source_init_global_info_handle info) -> CV2TableInitGlobalInfo * {
	return reinterpret_cast<CV2TableInitGlobalInfo *>(info);
}
static auto Convert(CV2TableInitGlobalInfo *info) -> duckdb_v2_data_source_init_global_info_handle {
	return reinterpret_cast<duckdb_v2_data_source_init_global_info_handle>(info);
}

class CV2TableInitLocalInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;
	const vector<column_t> *in_scan_columns = nullptr;

	duckdb_v2_opaque out_local_state = {};
};

static auto Convert(duckdb_v2_data_source_init_local_info_handle info) -> CV2TableInitLocalInfo * {
	return reinterpret_cast<CV2TableInitLocalInfo *>(info);
}
static auto Convert(CV2TableInitLocalInfo *info) -> duckdb_v2_data_source_init_local_info_handle {
	return reinterpret_cast<duckdb_v2_data_source_init_local_info_handle>(info);
}

class CV2TableExecInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;
	void *in_local_state = nullptr;
	const vector<column_t> *in_scan_columns = nullptr;

	DataChunk *output = nullptr;
	DUCKDB_V2_DATA_SOURCE_RESULT out_result = DUCKDB_V2_DATA_SOURCE_RESULT_IMPLICIT;
	vector<unique_ptr<AsyncTask>> out_tasks;
};

//! A task the exec callback added: the scan is blocked until it has run
class CV2DataSourceTask final : public AsyncTask {
public:
	CV2DataSourceTask(duckdb_v2_data_source_task_callback_fn task_cb, CV2UserData data)
	    : task_cb(task_cb), data(std::move(data)) {
	}

	void Execute() override {
		CV2ErrorInfo err = {};
		auto err_ptr = Convert(&err);
		task_cb(data.GetData(), &err_ptr);
		if (err.HasError()) {
			err.ThrowAsException();
		}
	}

private:
	duckdb_v2_data_source_task_callback_fn task_cb;
	CV2UserData data;
};

static auto Convert(duckdb_v2_data_source_exec_info_handle info) -> CV2TableExecInfo * {
	return reinterpret_cast<CV2TableExecInfo *>(info);
}
static auto Convert(CV2TableExecInfo *info) -> duckdb_v2_data_source_exec_info_handle {
	return reinterpret_cast<duckdb_v2_data_source_exec_info_handle>(info);
}

class CV2TableProgressInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;

	double out_progress = 0.0;
};

static auto Convert(duckdb_v2_data_source_progress_info_handle info) -> CV2TableProgressInfo * {
	return reinterpret_cast<CV2TableProgressInfo *>(info);
}
static auto Convert(CV2TableProgressInfo *info) -> duckdb_v2_data_source_progress_info_handle {
	return reinterpret_cast<duckdb_v2_data_source_progress_info_handle>(info);
}

class CV2TableMetricsInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;

	//! The metrics the callback reported, by name, applied to the scan's metrics afterwards
	vector<pair<string, uint64_t>> out_metrics;
};

//! The operator metrics a table function scan can report, by the name the profiler uses for them
static auto CV2TableApplyMetric(OperatorMetrics &metrics, const string &name, uint64_t value) -> bool {
	if (name == MetricOperatorRowsScanned::Name) {
		metrics.rows_scanned = value;
	} else if (name == MetricOperatorRowGroupsScanned::Name) {
		metrics.row_groups_scanned = value;
	} else if (name == MetricOperatorTotalRowGroupsToScan::Name) {
		metrics.total_row_groups_to_scan = value;
	} else if (name == MetricOperatorBytesScanned::Name) {
		metrics.bytes_scanned = value;
		metrics.bytes_scanned_reported = true;
	} else {
		return false;
	}
	return true;
}

static void CV2TableAddMetric(CV2TableMetricsInfo &info, const char *name, idx_t name_length, uint64_t value) {
	string metric_name(name, name_length);
	OperatorMetrics probe;
	if (!CV2TableApplyMetric(probe, metric_name, value)) {
		throw InvalidInputException("\"%s\" is not a metric a table function can report, expected one of "
		                            "\"%s\", \"%s\", \"%s\" or \"%s\"",
		                            metric_name, MetricOperatorRowsScanned::Name, MetricOperatorRowGroupsScanned::Name,
		                            MetricOperatorTotalRowGroupsToScan::Name, MetricOperatorBytesScanned::Name);
	}
	info.out_metrics.emplace_back(std::move(metric_name), value);
}

static auto Convert(duckdb_v2_data_source_metrics_info_handle info) -> CV2TableMetricsInfo * {
	return reinterpret_cast<CV2TableMetricsInfo *>(info);
}
static auto Convert(CV2TableMetricsInfo *info) -> duckdb_v2_data_source_metrics_info_handle {
	return reinterpret_cast<duckdb_v2_data_source_metrics_info_handle>(info);
}

class CV2TableClaimBatchInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;
	void *in_local_state = nullptr;

	bool out_claimed = false;
};

static auto Convert(duckdb_v2_data_source_claim_batch_info_handle info) -> CV2TableClaimBatchInfo * {
	return reinterpret_cast<CV2TableClaimBatchInfo *>(info);
}
static auto Convert(CV2TableClaimBatchInfo *info) -> duckdb_v2_data_source_claim_batch_info_handle {
	return reinterpret_cast<duckdb_v2_data_source_claim_batch_info_handle>(info);
}

class CV2TablePartitionDataInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	void *in_global_state = nullptr;
	void *in_local_state = nullptr;
	const OperatorPartitionInfo *in_partition_info = nullptr;
	const vector<column_t> *in_partition_columns = nullptr;
	const vector<LogicalType> *in_column_types = nullptr;

	optional_idx out_batch_index;
	vector<bool> out_partition_value_set;
	vector<Value> out_partition_values;
};

static auto Convert(duckdb_v2_data_source_partition_data_info_handle info) -> CV2TablePartitionDataInfo * {
	return reinterpret_cast<CV2TablePartitionDataInfo *>(info);
}
static auto Convert(CV2TablePartitionDataInfo *info) -> duckdb_v2_data_source_partition_data_info_handle {
	return reinterpret_cast<duckdb_v2_data_source_partition_data_info_handle>(info);
}

//! Virtual columns are told apart from declared ones in the column lists of the scan by this plus their value in the
//! public enum
static constexpr column_t CV2_VIRTUAL_COLUMN_START = UINT64_C(9223372036854775808);

//! The index the scan's column lists hold for a virtual column of the engine, which is invalid if there is no such
//! virtual column
static auto CV2VirtualColumnIndex(column_t engine_id) -> column_t {
	if (engine_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		return CV2_VIRTUAL_COLUMN_START + DUCKDB_V2_TABLE_VIRTUAL_COLUMN_FILE_ROW_NUMBER;
	}
	throw InternalException("Unsupported virtual column %llu in a table function scan", engine_id);
}

//! The index the function sees for a column of the scan. A virtual column of a function wrapped in a multi-file
//! function has an index of its own past the declared columns, and the wrapper tells which virtual column that is.
auto CV2ScanColumnIndex(column_t column, optional_ptr<const unordered_map<column_t, column_t>> virtual_columns)
    -> column_t {
	if (virtual_columns) {
		auto entry = virtual_columns->find(column);
		if (entry != virtual_columns->end()) {
			return CV2VirtualColumnIndex(entry->second);
		}
	}
	return IsVirtualColumn(column) ? CV2VirtualColumnIndex(column) : column;
}

//! The declared column behind each vector of the output chunk. With projection pushdown the engine sizes the chunk
//! to the columns it asked for; without it the chunk always holds every declared column, whatever the engine asked.
static auto CV2TableScanColumns(const CV2DataSourceBindData &bind_data, const TableFunctionInitInput &input)
    -> vector<column_t> {
	if (bind_data.scan->projection_pushdown) {
		if (input.column_ids.empty()) {
			// the scan only produces rows (e.g. for count(*)) - the function produces them through its first column
			return {0};
		}
		const auto &virtual_columns = TableFunctionFileInitInput::Get(input).virtual_columns;
		vector<column_t> columns;
		columns.reserve(input.column_ids.size());
		for (auto column : input.column_ids) {
			columns.push_back(CV2ScanColumnIndex(column, virtual_columns));
		}
		return columns;
	}
	vector<column_t> columns;
	for (idx_t i = 0; i < bind_data.column_types.size(); i++) {
		columns.push_back(i);
	}
	return columns;
}

//! The filters of the query that apply to a single column of the scan, as the position of the column among the scan's
//! columns and the predicate on it. A filter the engine applies speculatively is handed out as the predicate it
//! applies.
static auto CV2TableFilters(const TableFunctionInitInput &input) -> vector<pair<idx_t, Expression *>> {
	//! The predicate behind a filter that is applied speculatively, which wraps it
	auto unwrap = [](Expression &expression) -> Expression & {
		reference<Expression> current(expression);
		while (current.get().GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &function = current.get().Cast<BoundFunctionExpression>();
			if (!function.BindInfo()) {
				break;
			}
			auto &name = function.Function().GetName();
			if (name == OptionalFilterScalarFun::NAME) {
				current = *function.BindInfo()->Cast<OptionalFilterFunctionData>().child_filter_expr;
			} else if (name == SelectivityOptionalFilterScalarFun::NAME) {
				current = *function.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>().child_filter_expr;
			} else {
				break;
			}
		}
		return current.get();
	};
	vector<pair<idx_t, Expression *>> result;
	if (!input.filters) {
		return result;
	}
	for (auto &entry : *input.filters) {
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "CV2TableFilters");
		result.emplace_back(entry.GetIndex().GetIndex(), &unwrap(*filter.expr));
	}
	return result;
}

//! Offers the filters the scan applies to the filter pushdown callback once more, for the scan that is about to start.
//! The callback only sees the predicates at optimization time, which a function wrapped in a multi-file function does
//! not: the optimizer asks the wrapper. The engine applies the filters to what the function produces, so accepting a
//! predicate has no effect here.
static auto CV2TableOfferFilters(ClientContext &context, const CV2DataSourceBindData &bind_data,
                                 const TableFunctionInitInput &input) -> void {
	const auto &scan = *bind_data.scan;
	const auto &file_input = TableFunctionFileInitInput::Get(input);
	auto filters = CV2TableFilters(input);
	if (file_input.expression_map) {
		// the predicate on a converted column is written against the converted values, not those of the file
		vector<pair<idx_t, Expression *>> on_file_columns;
		for (auto &entry : filters) {
			if (file_input.expression_map->find(ProjectionIndex(entry.first)) == file_input.expression_map->end()) {
				on_file_columns.push_back(entry);
			}
		}
		filters = std::move(on_file_columns);
	}
	if (filters.empty()) {
		return;
	}
	// A table filter refers to the one column it applies to by a physical reference; the callback is offered column
	// references, like at optimization time, which it resolves through the column list
	vector<unique_ptr<Expression>> predicates;
	for (auto &entry : filters) {
		auto predicate = entry.second->Copy();
		ExpressionIterator::EnumerateExpression(predicate, [&](unique_ptr<Expression> &child) {
			if (child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
				child = make_uniq<BoundColumnRefExpression>(child->GetAlias(), child->GetReturnType(),
				                                            ColumnBinding(TableIndex(0), ProjectionIndex(entry.first)));
			}
		});
		predicates.push_back(std::move(predicate));
	}

	CV2TableFilterPushdownInfo args = {};
	args.in_user_data = scan.user_data ? scan.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_filters = &predicates;
	for (auto column : input.column_ids) {
		args.in_column_ids.push_back(CV2ScanColumnIndex(column, file_input.virtual_columns));
	}
	args.out_handled.resize(predicates.size(), false);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	scan.filter_pushdown_cb(Convert(&args), Convert(&callback_context), &err_ptr);
	if (err.HasError()) {
		err.ThrowAsException();
	}
}

auto CV2DataSourceInitGlobal(ClientContext &context, TableFunctionInitInput &input)
    -> unique_ptr<GlobalTableFunctionState> {
	const auto &bind_data = input.bind_data->Cast<CV2DataSourceBindData>();
	const auto &info = *bind_data.source;

	// Always produced, even without a callback: it carries the thread count and column list the scan runs with.
	auto result = make_uniq<CV2DataSourceGlobalState>();
	result->scan_columns = CV2TableScanColumns(bind_data, input);
	if (bind_data.scan->ReadsFilters()) {
		CV2TableOfferFilters(context, bind_data, input);
	}
	if (!info.init_global_cb) {
		return std::move(result);
	}

	CV2TableInitGlobalInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_scan_columns = &result->scan_columns;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.init_global_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (args.out_global_state.ptr) {
		result->handle =
		    CV2UserData(args.out_global_state.ptr, args.out_global_state.destroy, args.out_global_state.equals);
	}
	result->max_threads = args.out_max_threads;

	// Throw after taking ownership of the state, so that it is destroyed even if we error.
	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(result);
}

//! Prepares the conversions the multi-file scan asks for (see TableFunctionFileInitInput::expression_map). The function
//! produces the values of its file, and the conversion turns them into the values the filters are written against.
static auto CV2TableInitConvertedColumns(ClientContext &context, const CV2DataSourceBindData &bind_data,
                                         const TableFunctionInitInput &input, const vector<column_t> &scan_columns,
                                         CV2DataSourceLocalState &local_state) -> void {
	const auto &expression_map = TableFunctionFileInitInput::Get(input).expression_map;
	if (!expression_map || expression_map->empty()) {
		return;
	}
	vector<LogicalType> file_types;
	for (auto column : scan_columns) {
		file_types.push_back(IsVirtualColumn(column) ? LogicalType::BIGINT : bind_data.column_types[column]);
	}
	for (auto &entry : *expression_map) {
		const auto position = entry.first.GetIndex();
		const auto input_count = entry.second.column_indexes.size();
		if (position + input_count > file_types.size()) {
			throw InternalException("Converted column %llu of a table function scan is out of range", position);
		}
		CV2DataSourceLocalState::ConvertedColumn converted;
		converted.position = position;
		converted.expression = entry.second.expression->Copy();
		converted.executor = make_uniq<ExpressionExecutor>(context, *converted.expression);
		vector<LogicalType> input_types(file_types.begin() + NumericCast<int64_t>(position),
		                                file_types.begin() + NumericCast<int64_t>(position + input_count));
		converted.input = make_uniq<DataChunk>();
		converted.input->InitializeEmpty(input_types);
		local_state.converted_columns.push_back(std::move(converted));
	}
	local_state.converted_position.assign(file_types.size(), false);
	for (auto &converted : local_state.converted_columns) {
		local_state.converted_position[converted.position] = true;
	}
	local_state.file_chunk = make_uniq<DataChunk>();
	local_state.file_chunk->Initialize(context, file_types);
}

//! Produces the converted columns from what the function produced in the types of its file
static auto CV2TableConvertColumns(CV2DataSourceLocalState &local_state, DataChunk &output) -> void {
	auto &file_chunk = *local_state.file_chunk;
	D_ASSERT(local_state.converted_position.size() == output.ColumnCount());
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		if (!local_state.converted_position[i]) {
			output.data[i].Reference(file_chunk.data[i]);
		}
	}
	for (auto &converted : local_state.converted_columns) {
		auto &converted_input = *converted.input;
		for (idx_t i = 0; i < converted_input.ColumnCount(); i++) {
			converted_input.data[i].Reference(file_chunk.data[converted.position + i]);
		}
		converted_input.SetChildCardinality(file_chunk.size());
		converted.executor->ExecuteExpression(converted_input, output.data[converted.position]);
	}
	output.SetChildCardinality(file_chunk.size());
}

auto CV2DataSourceInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                            GlobalTableFunctionState *global_state) -> unique_ptr<LocalTableFunctionState> {
	const auto &bind_data = input.bind_data->Cast<CV2DataSourceBindData>();
	const auto &info = *bind_data.source;

	// Always produced, even without a callback: get_partition_data keeps its per-thread guard state here.
	auto result = make_uniq<CV2DataSourceLocalState>();
	if (bind_data.scan->ReadsFilters() && input.filters) {
		// The function only reads the filters, so it is left to the engine to apply them to what it produces
		for (auto &entry : *input.filters) {
			result->filters.emplace_back(entry.GetIndex().GetIndex(),
			                             TableFilterState::Initialize(context.client, entry.Filter()));
		}
	}
	auto scan_columns = CV2TableScanColumns(bind_data, input);
	if (bind_data.scan->ReadsFilters()) {
		CV2TableInitConvertedColumns(context.client, bind_data, input, scan_columns, *result);
	}
	if (!info.init_local_cb) {
		return std::move(result);
	}

	CV2TableInitLocalInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_scan_columns = &scan_columns;
	if (global_state) {
		args.in_global_state = global_state->Cast<CV2DataSourceGlobalState>().handle.GetData();
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context.client);
	info.init_local_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (args.out_local_state.ptr) {
		result->handle =
		    CV2UserData(args.out_local_state.ptr, args.out_local_state.destroy, args.out_local_state.equals);
	}

	// Throw after taking ownership of the state, so that it is destroyed even if we error.
	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(result);
}

//! Calls the claim batch callback for the local state of the given input - returns whether a batch was claimed
static auto CV2TableClaimNextBatch(ClientContext &context, TableFunctionInput &input) -> bool {
	const auto &bind_data = input.bind_data->Cast<CV2DataSourceBindData>();
	const auto &info = *bind_data.source;
	auto &local_state = input.local_state->Cast<CV2DataSourceLocalState>();

	CV2TableClaimBatchInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	if (input.global_state) {
		args.in_global_state = input.global_state->Cast<CV2DataSourceGlobalState>().handle.GetData();
	}
	args.in_local_state = local_state.handle.GetData();

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.claim_batch_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
	local_state.batch_claimed = args.out_claimed;
	return args.out_claimed;
}

//! The caller claims the batches of this local state itself - every exec then scans the claimed batch only
auto CV2DataSourceClaimBatch(ClientContext &context, TableFunctionInput &input) -> bool {
	input.local_state->Cast<CV2DataSourceLocalState>().claimed_externally = true;
	return CV2TableClaimNextBatch(context, input);
}

static auto CV2TableExecBatch(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void;

static auto CV2DataSourceExecClaimed(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void {
	const auto &info = *input.bind_data->Cast<CV2DataSourceBindData>().source;
	if (!info.claim_batch_cb) {
		CV2TableExecBatch(context, input, output);
		return;
	}
	auto &local_state = input.local_state->Cast<CV2DataSourceLocalState>();
	if (local_state.claimed_externally) {
		// an empty chunk ends the batch, after which the caller claims the next one
		CV2TableExecBatch(context, input, output);
		return;
	}
	// nobody claims the batches for us - move on to the next batch whenever the current one is exhausted
	while (true) {
		if (!local_state.batch_claimed && !CV2TableClaimNextBatch(context, input)) {
			return;
		}
		CV2TableExecBatch(context, input, output);
		if (output.size() > 0 || local_state.result == DUCKDB_V2_DATA_SOURCE_RESULT_BLOCKED) {
			return;
		}
		local_state.batch_claimed = false;
		if (local_state.result == DUCKDB_V2_DATA_SOURCE_RESULT_FINISHED) {
			return;
		}
	}
}

//! Checks the result the exec callback declared against the batch it produced
static auto CheckDeclaredResult(const CV2DataSourceLocalState &local_state, idx_t row_count) -> void {
	switch (local_state.result) {
	case DUCKDB_V2_DATA_SOURCE_RESULT_IMPLICIT:
		break;
	case DUCKDB_V2_DATA_SOURCE_RESULT_HAVE_MORE_OUTPUT:
		if (row_count == 0) {
			throw InvalidInputException("A data source declared that its scan has more output, but produced no rows");
		}
		break;
	case DUCKDB_V2_DATA_SOURCE_RESULT_FINISHED:
		if (row_count > 0) {
			throw InvalidInputException("A data source declared that its scan is finished, but produced rows");
		}
		break;
	case DUCKDB_V2_DATA_SOURCE_RESULT_BLOCKED:
		if (row_count > 0) {
			throw InvalidInputException("A data source declared that its scan is blocked, but produced rows");
		}
		if (local_state.tasks.empty()) {
			throw InvalidInputException("A data source declared that its scan is blocked, but added no task to run");
		}
		break;
	default:
		throw InvalidInputException("A data source declared an unknown result: %d",
		                            static_cast<int>(local_state.result));
	}
	if (!local_state.tasks.empty() && local_state.result != DUCKDB_V2_DATA_SOURCE_RESULT_BLOCKED) {
		throw InvalidInputException("A data source added a task to run, but declared that its scan is not blocked");
	}
}

auto CV2DataSourceExec(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void {
	auto &local_state = input.local_state->Cast<CV2DataSourceLocalState>();
	while (true) {
		// a pass that claims no batch does not invoke the callback, and declares nothing
		local_state.result = DUCKDB_V2_DATA_SOURCE_RESULT_IMPLICIT;
		local_state.tasks.clear();
		CV2DataSourceExecClaimed(context, input, output);
		CheckDeclaredResult(local_state, output.size());
		switch (local_state.result) {
		case DUCKDB_V2_DATA_SOURCE_RESULT_HAVE_MORE_OUTPUT:
			input.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
			return;
		case DUCKDB_V2_DATA_SOURCE_RESULT_FINISHED:
			input.async_result = AsyncResultType::FINISHED;
			return;
		case DUCKDB_V2_DATA_SOURCE_RESULT_BLOCKED: {
			AsyncResult blocked(std::move(local_state.tasks));
			local_state.tasks.clear();
			if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				// the scan reschedules once the tasks have run, which calls the function again
				input.async_result = std::move(blocked);
				return;
			}
			// a synchronous scan cannot block: run the tasks here, then call the function again
			blocked.ExecuteTasksSynchronously();
			context.FastInterruptCheck();
			break;
		}
		default:
			return;
		}
	}
}

static auto CV2TableExecBatchOnce(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void;

//! Runs the exec callback and, for a function that reads the filters of the query, applies them to what it produced:
//! the function is free to produce rows that do not pass them
static auto CV2TableExecBatch(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void {
	auto &local_state = input.local_state->Cast<CV2DataSourceLocalState>();
	while (true) {
		if (local_state.file_chunk) {
			local_state.file_chunk->Reset();
			CV2TableExecBatchOnce(context, input, *local_state.file_chunk);
			CV2TableConvertColumns(local_state, output);
		} else {
			CV2TableExecBatchOnce(context, input, output);
		}
		if (local_state.filters.empty() || output.size() == 0) {
			return;
		}
		const auto count = output.size();
		idx_t remaining = count;
		SelectionVector selection;
		selection.Initialize(nullptr);
		for (auto &filter : local_state.filters) {
			if (remaining == 0) {
				break;
			}
			ColumnSegment::FilterSelection(selection, output.data[filter.first], *filter.second, count, remaining);
		}
		if (remaining == count) {
			return;
		}
		if (remaining > 0) {
			output.Slice(selection, remaining);
			return;
		}
		// Nothing of this batch passed, but an empty chunk means that the batch is exhausted
		output.Reset();
		context.FastInterruptCheck();
	}
}

static auto CV2TableExecBatchOnce(ClientContext &context, TableFunctionInput &input, DataChunk &output) -> void {
	const auto &bind_data = input.bind_data->Cast<CV2DataSourceBindData>();
	const auto &info = *bind_data.source;

	CV2TableExecInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	if (input.global_state) {
		auto &global_state = input.global_state->Cast<CV2DataSourceGlobalState>();
		args.in_global_state = global_state.handle.GetData();
		args.in_scan_columns = &global_state.scan_columns;
	}
	auto &local_state = input.local_state->Cast<CV2DataSourceLocalState>();
	args.in_local_state = local_state.handle.GetData();
	reference<DataChunk> target(output);
	if (output.ColumnCount() == 0) {
		// the scan requests no columns (e.g. for count(*)), only rows - the function writes them into its first column
		if (!local_state.row_chunk) {
			local_state.row_chunk = make_uniq<DataChunk>();
			local_state.row_chunk->Initialize(context, {bind_data.column_types[0]});
		}
		local_state.row_chunk->Reset();
		target = *local_state.row_chunk;
	}
	args.output = &target.get();

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.exec_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	local_state.result = args.out_result;
	local_state.tasks = std::move(args.out_tasks);
	if (err.HasError()) {
		err.ThrowAsException();
	}

	// The vector write API sizes per vector, while the engine reads the chunk's cardinality. Callers typically size
	// only the first vector and write the rest through direct buffer writes, so take the first vector's size as the
	// batch's row count and propagate it to the others. A table function always has at least one result column.
	auto row_count = target.get().data[0].size();
	if (output.ColumnCount() == 0) {
		output.SetCardinalityUnsafe(row_count);
		return;
	}
	output.SetChildCardinality(row_count);
}

static auto CV2DataSourceProgress(ClientContext &context, const FunctionData *bind_data,
                                  const GlobalTableFunctionState *global_state) -> double {
	const auto &data = bind_data->Cast<CV2DataSourceBindData>();
	const auto &info = *data.source;

	CV2TableProgressInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = data.handle ? data.handle->GetData() : nullptr;
	if (global_state) {
		// The callback runs concurrently with the scan, so the state it reads is the shared one, unsynchronized.
		args.in_global_state = global_state->Cast<CV2DataSourceGlobalState>().handle.GetData();
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.progress_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	// The API contract is a 0.0-1.0 fraction, the engine reads a 0-100 percentage.
	return MinValue<double>(MaxValue<double>(args.out_progress, 0.0), 1.0) * 100.0;
}

static auto CV2DataSourceGetMetrics(TableFunctionGetMetricsInput &input) -> void {
	if (!input.bind_data) {
		return;
	}
	const auto &data = input.bind_data->Cast<CV2DataSourceBindData>();
	const auto &info = *data.source;

	CV2TableMetricsInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = data.handle ? data.handle->GetData() : nullptr;
	if (input.global_state) {
		// The callback can run while the scan does, so the state it reads is the shared one, unsynchronized.
		args.in_global_state = input.global_state->Cast<CV2DataSourceGlobalState>().handle.GetData();
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(input.context);
	info.metrics_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	for (auto &metric : args.out_metrics) {
		CV2TableApplyMetric(input.operator_metrics, metric.first, metric.second);
	}
}

//! Batch index is required on every call, even when unrequested: the pipeline range-checks it regardless.
static auto CV2TakePartitionData(CV2TablePartitionDataInfo &args, const Identifier &function_name)
    -> OperatorPartitionData {
	if (!args.out_batch_index.IsValid()) {
		throw InvalidInputException("The partition data callback of table function %s did not set a batch index.",
		                            function_name);
	}
	OperatorPartitionData result(args.out_batch_index.GetIndex());
	result.partition_data.reserve(args.out_partition_values.size());
	for (idx_t i = 0; i < args.out_partition_values.size(); i++) {
		if (!args.out_partition_value_set[i]) {
			throw InvalidInputException("The partition data callback of table function %s did not set the "
			                            "partitioning column value at index %llu.",
			                            function_name, i);
		}
		result.partition_data.emplace_back(std::move(args.out_partition_values[i]));
	}
	return result;
}

static auto CV2DataSourceGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input)
    -> OperatorPartitionData {
	const auto &bind_data = input.bind_data->Cast<CV2DataSourceBindData>();
	const auto &info = *bind_data.source;

	auto &global_state = input.global_state->Cast<CV2DataSourceGlobalState>();
	auto &local_state = input.local_state->Cast<CV2DataSourceLocalState>();

	CV2TablePartitionDataInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_global_state = global_state.handle.GetData();
	args.in_local_state = local_state.handle.GetData();
	args.in_partition_info = &input.partition_info;
	// The planner resolves these to scan positions, unlike get_partition_info's, which it resolves to declared
	// columns; the callback is promised declared columns in both.
	args.in_partition_columns = &local_state.PartitionColumns(input.partition_info, global_state.scan_columns);
	args.in_column_types = &bind_data.column_types;
	args.out_partition_value_set.resize(args.in_partition_columns->size(), false);
	args.out_partition_values.resize(args.in_partition_columns->size());

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.partition_data_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	auto result = CV2TakePartitionData(args, info.name);
	local_state.CheckPartitionData(info.name, result);
	return result;
}

void CV2DataSourceInfo::Verify() const {
	if (!exec_cb) {
		throw InvalidInputException("Exec callback must be set for the data source.");
	}
}

void CV2DataSourceInfo::ConfigureScan(TableFunction &function) const {
	function.function = CV2DataSourceExec;
	function.init_global = CV2DataSourceInitGlobal;
	function.init_local = CV2DataSourceInitLocal;
	if (progress_cb) {
		function.table_scan_progress = CV2DataSourceProgress;
	}
	if (partition_data_cb) {
		function.get_partition_data = CV2DataSourceGetPartitionData;
	}
	if (metrics_cb) {
		function.get_metrics = CV2DataSourceGetMetrics;
	}
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_global_get_user_data(duckdb_v2_data_source_init_global_info_handle info, void **data,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_global_get_bind_data(duckdb_v2_data_source_init_global_info_handle info, void **data,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_global_set_global_state(duckdb_v2_data_source_init_global_info_handle info,
                                                   duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_global_state = *data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_global_set_max_threads(duckdb_v2_data_source_init_global_info_handle info, idx_t max_threads,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		if (max_threads == 0) {
			throw duckdb::InvalidInputException("The maximum number of threads must be at least 1");
		}
		Convert(info)->out_max_threads = max_threads;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_global_get_column_count(duckdb_v2_data_source_init_global_info_handle info, idx_t *count,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_scan_columns->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_global_get_column_index(duckdb_v2_data_source_init_global_info_handle info, idx_t index,
                                                   idx_t *column_index, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_scan_columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_data_source_init_global_get_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_source_init_local_get_user_data(duckdb_v2_data_source_init_local_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_init_local_get_bind_data(duckdb_v2_data_source_init_local_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_local_get_global_state(duckdb_v2_data_source_init_local_info_handle info, void **data,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_local_set_local_state(duckdb_v2_data_source_init_local_info_handle info,
                                                 duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { Convert(info)->out_local_state = *data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_local_get_column_count(duckdb_v2_data_source_init_local_info_handle info, idx_t *count,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_scan_columns->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_init_local_get_column_index(duckdb_v2_data_source_init_local_info_handle info, idx_t index,
                                                  idx_t *column_index, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_scan_columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_data_source_init_local_get_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_get_user_data(duckdb_v2_data_source_exec_info_handle info, void **data,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_get_bind_data(duckdb_v2_data_source_exec_info_handle info, void **data,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_get_global_state(duckdb_v2_data_source_exec_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_get_local_state(duckdb_v2_data_source_exec_info_handle info, void **data,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_local_state; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_get_output_chunk(duckdb_v2_data_source_exec_info_handle info,
                                                            duckdb_v2_data_chunk_handle *chunk,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(chunk);
	return WithErrorHandler(err, [&]() { *chunk = Convert(Convert(info)->output); });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_set_result(duckdb_v2_data_source_exec_info_handle info,
                                                      DUCKDB_V2_DATA_SOURCE_RESULT result,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		switch (result) {
		case DUCKDB_V2_DATA_SOURCE_RESULT_IMPLICIT:
		case DUCKDB_V2_DATA_SOURCE_RESULT_HAVE_MORE_OUTPUT:
		case DUCKDB_V2_DATA_SOURCE_RESULT_FINISHED:
		case DUCKDB_V2_DATA_SOURCE_RESULT_BLOCKED:
			Convert(info)->out_result = result;
			break;
		default:
			throw duckdb::InvalidInputException("Unknown data source result: %d", static_cast<int>(result));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_add_task(duckdb_v2_data_source_exec_info_handle info,
                                                    duckdb_v2_data_source_task_callback_fn task, duckdb_v2_opaque *data,
                                                    duckdb_v2_error_info_handle *err) {
	// the data is taken over, also when the call fails
	CV2UserData task_data;
	if (data) {
		task_data = CV2UserData(data->ptr, data->destroy);
	}
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(task);
	return WithErrorHandler(err, [&]() {
		auto &exec_info = *Convert(info);
		exec_info.out_tasks.push_back(duckdb::make_uniq<CV2DataSourceTask>(task, std::move(task_data)));
		exec_info.out_result = DUCKDB_V2_DATA_SOURCE_RESULT_BLOCKED;
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_get_column_count(duckdb_v2_data_source_exec_info_handle info, idx_t *count,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_scan_columns->size(); });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_exec_get_column_index(duckdb_v2_data_source_exec_info_handle info, idx_t index,
                                                            idx_t *column_index, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_scan_columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException("Index out of bounds in duckdb_v2_data_source_exec_get_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_source_progress_get_user_data(duckdb_v2_data_source_progress_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_progress_get_bind_data(duckdb_v2_data_source_progress_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_progress_get_global_state(duckdb_v2_data_source_progress_info_handle info,
                                                                void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_progress_set_progress(duckdb_v2_data_source_progress_info_handle info,
                                                            double progress, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_progress = progress; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_metrics_get_user_data(duckdb_v2_data_source_metrics_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_metrics_get_bind_data(duckdb_v2_data_source_metrics_info_handle info, void **data,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_metrics_get_global_state(duckdb_v2_data_source_metrics_info_handle info,
                                                               void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_metrics_set(duckdb_v2_data_source_metrics_info_handle info, const char *name,
                                                  idx_t name_length, uint64_t value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() { CV2TableAddMetric(*Convert(info), name, name_length, value); });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_get_user_data(duckdb_v2_data_source_partition_data_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_get_bind_data(duckdb_v2_data_source_partition_data_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_get_global_state(duckdb_v2_data_source_partition_data_info_handle info,
                                                      void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_get_local_state(duckdb_v2_data_source_partition_data_info_handle info, void **data,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_local_state; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_requires_batch_index(duckdb_v2_data_source_partition_data_info_handle info,
                                                          bool *required, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(required);
	return WithErrorHandler(err, [&]() { *required = Convert(info)->in_partition_info->RequiresBatchIndex(); });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_requires_partition_columns(duckdb_v2_data_source_partition_data_info_handle info,
                                                                bool *required, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(required);
	return WithErrorHandler(err, [&]() { *required = Convert(info)->in_partition_info->RequiresPartitionColumns(); });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_get_partition_column_count(duckdb_v2_data_source_partition_data_info_handle info,
                                                                idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_partition_columns->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_get_partition_column_index(duckdb_v2_data_source_partition_data_info_handle info,
                                                                idx_t index, idx_t *column_index,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_partition_columns;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_data_source_partition_data_get_partition_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_set_batch_index(duckdb_v2_data_source_partition_data_info_handle info,
                                                     idx_t batch_index, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		// ToPipelinePartitionData adds base + 1 and throws an InternalException from BATCH_INCREMENT - 1 on.
		if (batch_index >= duckdb::PipelineBuildState::BATCH_INCREMENT - 2) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_data_source_partition_data_set_batch_index: batch_index %llu must be less than "
			    "%llu",
			    batch_index, duckdb::PipelineBuildState::BATCH_INCREMENT - 2);
		}
		Convert(info)->out_batch_index = batch_index;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_partition_data_set_partition_value(duckdb_v2_data_source_partition_data_info_handle info,
                                                         idx_t index, duckdb_v2_value_handle value,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto &args = *Convert(info);
		if (index >= args.out_partition_values.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_data_source_partition_data_set_partition_value");
		}
		const auto &expected_type = (*args.in_column_types)[(*args.in_partition_columns)[index]];
		const auto &actual = *Convert(value);
		if (actual.type() != expected_type) {
			throw duckdb::InvalidInputException(
			    "duckdb_v2_data_source_partition_data_set_partition_value: expected type %s for the "
			    "partitioning column at index %llu, got %s",
			    expected_type.ToString(), index, actual.type().ToString());
		}
		args.out_partition_values[index] = actual;
		args.out_partition_value_set[index] = true;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_claim_batch_get_user_data(duckdb_v2_data_source_claim_batch_info_handle info, void **data,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_claim_batch_get_bind_data(duckdb_v2_data_source_claim_batch_info_handle info, void **data,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_claim_batch_get_global_state(duckdb_v2_data_source_claim_batch_info_handle info, void **data,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_global_state; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_claim_batch_get_local_state(duckdb_v2_data_source_claim_batch_info_handle info, void **data,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_local_state; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_claim_batch_set_claimed(duckdb_v2_data_source_claim_batch_info_handle info,
                                                              bool claimed, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_claimed = claimed; });
}

//----------------------------------------------------------------------------------------------------------------------
// Data sources
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_data_source_create(duckdb_v2_factory_handle factory, duckdb_v2_data_source_handle *source,
                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(source);
	*source = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2DataSource>(Convert(factory)->GetDatabase());
		*source = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_source_set_user_data(duckdb_v2_data_source_handle source, duckdb_v2_opaque *data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(source)->info.user_data = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_set_init_global_callback(duckdb_v2_data_source_handle source,
                                               duckdb_v2_data_source_init_global_callback_fn callback,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() { Convert(source)->info.init_global_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_set_init_local_callback(duckdb_v2_data_source_handle source,
                                                              duckdb_v2_data_source_init_local_callback_fn callback,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() { Convert(source)->info.init_local_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_set_exec_callback(duckdb_v2_data_source_handle source,
                                                        duckdb_v2_data_source_exec_callback_fn callback,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() { Convert(source)->info.exec_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_set_progress_callback(duckdb_v2_data_source_handle source,
                                                            duckdb_v2_data_source_progress_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() { Convert(source)->info.progress_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_set_metrics_callback(duckdb_v2_data_source_handle source,
                                                           duckdb_v2_data_source_metrics_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() { Convert(source)->info.metrics_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_set_partition_data_callback(duckdb_v2_data_source_handle source,
                                                  duckdb_v2_data_source_partition_data_callback_fn callback,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() { Convert(source)->info.partition_data_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_data_source_set_claim_batch_callback(duckdb_v2_data_source_handle source,
                                               duckdb_v2_data_source_claim_batch_callback_fn callback,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() { Convert(source)->info.claim_batch_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_data_source_destroy(duckdb_v2_data_source_handle *source) {
	return WithErrorHandler(nullptr, [&]() {
		if (!source) {
			return;
		}
		if (*source) {
			delete Convert(*source);
			*source = nullptr;
		}
	});
}
