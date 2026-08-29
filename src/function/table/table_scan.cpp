#include "duckdb/function/table/table_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/storage_compatibility.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"

namespace duckdb {

//! One touched physical vector in a position-list scan.
struct PositionScanWindow {
	idx_t window_start;
	idx_t position_begin;
	idx_t candidate_count;
};

//! Adjacent windows that together produce at most one output vector.
struct PositionScanBatch {
	idx_t window_begin;
	idx_t window_count;
	idx_t candidate_count;
};

struct PositionScanData {
	//! Pin the RowGroup tree because checkpoint can replace it after physical windows are planned.
	shared_ptr<RowGroupSegmentTree> row_groups;
	//! Track the live row-ID boundary because append rollback can shorten the pinned tree between batches.
	shared_ptr<RowGroupCollection> row_group_collection;
	//! Absolute, ascending, unique persistent ART row IDs and their physical-window plan.
	vector<row_t> positions;
	vector<PositionScanWindow> windows;
	vector<PositionScanBatch> batches;
	atomic<idx_t> completed_rows {0};
};

struct PositionScanLocalState {
	//! Persistent scratch data for batches containing multiple windows.
	DataChunk window_chunk;
	//! The persistent scan state is pinned to the RowGroup tree used for planning.
	TableScanState scan_state;
};

static void BuildPositionScanBatches(const vector<PositionScanWindow> &windows, vector<PositionScanBatch> &batches) {
	D_ASSERT(batches.empty());
	for (idx_t window_idx = 0; window_idx < windows.size(); window_idx++) {
		auto &window = windows[window_idx];
		if (batches.empty() || batches.back().candidate_count + window.candidate_count > STANDARD_VECTOR_SIZE) {
			batches.push_back({window_idx, 0, 0});
		}
		auto &batch = batches.back();
		batch.window_count++;
		batch.candidate_count += window.candidate_count;
	}
}

static void BuildPositionScanPlan(RowGroupSegmentTree &row_groups, vector<row_t> &positions,
                                  vector<PositionScanWindow> &windows, vector<PositionScanBatch> &batches) {
	optional_ptr<SegmentNode<RowGroup>> row_group;
	idx_t cached_window_start = 0;
	idx_t cached_window_end = 0;
	idx_t input_idx = 0;
	idx_t output_idx = 0;
	while (input_idx < positions.size()) {
		D_ASSERT(input_idx == 0 || positions[input_idx - 1] < positions[input_idx]);
		const auto row_index = NumericCast<idx_t>(positions[input_idx]);
		if (row_index < cached_window_start || row_index >= cached_window_end) {
			if (!row_group || row_index < row_group->GetRowStart() || row_index >= row_group->GetRowEnd()) {
				auto tree_lock = row_groups.Lock();
				idx_t segment_idx;
				// Match Index Fetch by skipping row IDs absent from this captured tree during concurrent append.
				if (!row_groups.TryGetSegmentIndex(tree_lock, row_index, segment_idx)) {
					input_idx++;
					continue;
				}
				row_group = row_groups.GetSegmentByIndex(tree_lock, UnsafeNumericCast<int64_t>(segment_idx));
			}

			const auto row_group_offset = row_index - row_group->GetRowStart();
			const auto physical_start = row_group_offset / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE;
			cached_window_start = row_group->GetRowStart() + physical_start;
			const auto physical_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, row_group->GetCount() - physical_start);
			cached_window_end = cached_window_start + physical_count;
		}
		if (windows.empty() || windows.back().window_start != cached_window_start) {
			windows.push_back({cached_window_start, output_idx, 0});
		}
		positions[output_idx++] = positions[input_idx++];
		windows.back().candidate_count++;
	}
	positions.resize(output_idx);
	BuildPositionScanBatches(windows, batches);
}

static void ScanPositionWindow(DuckTransaction &transaction, CollectionScanState &scan_state,
                               const vector<row_t> &positions, const PositionScanWindow &window, idx_t live_row_end,
                               ColumnFetchState &fetch_state, DataChunk &result) {
	if (window.window_start >= live_row_end) {
		return;
	}
	auto row_group = scan_state.row_group;
	if (!row_group || window.window_start < row_group->GetRowStart() || window.window_start >= row_group->GetRowEnd()) {
		auto tree_lock = scan_state.row_groups->Lock();
		row_group = scan_state.row_groups->GetSegment(tree_lock, window.window_start);
	}
	const auto physical_start = window.window_start - row_group->GetRowStart();
	D_ASSERT(physical_start < row_group->GetCount());
	const auto live_count = live_row_end - window.window_start;
	const auto physical_count =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, MinValue<idx_t>(row_group->GetCount() - physical_start, live_count));
	auto &position_sel = scan_state.valid_sel;

	idx_t offsets[STANDARD_VECTOR_SIZE];
	idx_t candidate_count = 0;
	for (; candidate_count < window.candidate_count; candidate_count++) {
		const auto row_id = NumericCast<idx_t>(positions[window.position_begin + candidate_count]);
		if (row_id >= live_row_end) {
			break;
		}
		const auto position = row_id - window.window_start;
		D_ASSERT(position < physical_count);
		position_sel.set_index(candidate_count, position);
		offsets[candidate_count] = physical_start + position;
	}
	if (candidate_count == 0) {
		return;
	}

	sel_t visible_buffer[STANDARD_VECTOR_SIZE];
	SelectionVector visible_sel(visible_buffer, STANDARD_VECTOR_SIZE);
	auto &current_row_group = row_group->GetNode();
	const auto visible_count =
	    current_row_group.Fetch(TransactionData(transaction), offsets, candidate_count, visible_sel);
	if (visible_count != candidate_count) {
		position_sel.SliceInPlace(visible_sel, visible_count);
	}
	if (visible_count == 0) {
		return;
	}
	ScanOptions options {TransactionData(transaction)};
	current_row_group.ScanPositions(options, scan_state, *row_group, physical_start / STANDARD_VECTOR_SIZE,
	                                physical_count, visible_count, fetch_state, result);
}

struct TableScanLocalState : public LocalTableFunctionState {
	//! The current position in the scan.
	TableScanState scan_state;
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;

	idx_t rows_in_current_row_group = 0;
	idx_t row_groups_scanned = 0;
};

struct IndexScanLocalState : public LocalTableFunctionState {
	//! The batch index, which determines the offset in the row ID vector.
	idx_t batch_index;
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;
	//! The row fetch state.
	ColumnFetchState fetch_state;
	//! The current position in the local storage scan.
	TableScanState scan_state;
	bool in_charge_of_final_stretch {false};
	idx_t rows_scanned = 0;
	unique_ptr<PositionScanLocalState> position_state;
};

class TableScanGlobalState : public GlobalTableFunctionState {
public:
	TableScanGlobalState(ClientContext &context, const FunctionData *bind_data_p) {
		D_ASSERT(bind_data_p);
		auto &bind_data = bind_data_p->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &storage = duck_table.GetStorage();
		max_threads = storage.MaxThreads(context);
		total_row_groups_to_scan = storage.GetRowGroupCountWithLocalStorage(context);
	}

	//! The maximum number of threads for this table scan.
	idx_t max_threads;
	//! The total number of row groups available to this table scan.
	idx_t total_row_groups_to_scan;
	//! The projected columns of this table scan.
	vector<idx_t> projection_ids;
	//! The types of all scanned columns.
	vector<LogicalType> scanned_types;

public:
	virtual unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                           TableFunctionInitInput &input) = 0;
	virtual void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) = 0;
	virtual double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const = 0;
	virtual OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                        TableFunctionGetPartitionInput &input) = 0;
	virtual idx_t TableScanRowsScanned(LocalTableFunctionState &state) = 0;
	virtual idx_t TableScanRowGroupsScanned(LocalTableFunctionState &state) = 0;

	idx_t MaxThreads() const override {
		return max_threads;
	}
	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

class DuckIndexScanState : public TableScanGlobalState {
public:
	DuckIndexScanState(ClientContext &context, const FunctionData *bind_data_p)
	    : TableScanGlobalState(context, bind_data_p), next_batch_index(0), arena(Allocator::Get(context)),
	      row_ids(nullptr), row_id_count(0) {
	}

	//! The index of the next persistent batch.
	atomic<idx_t> next_batch_index;
	//! The arena allocator containing the memory of the row IDs.
	ArenaAllocator arena;
	//! A pointer to the row IDs.
	row_t *row_ids;
	//! The number of scanned row IDs.
	idx_t row_id_count;
	//! The column IDs of the to-be-scanned columns.
	vector<StorageIndex> column_ids;
	//! Keep ART rowids and row-group trees paired while rowid-shifting index vacuum can run.
	unique_ptr<StorageLockKey> vacuum_lock;
	//! Non-null when persistent ART row IDs use position-list materialization.
	unique_ptr<PositionScanData> position_data;

public:
	unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                   TableFunctionInitInput &input) override {
		auto l_state = make_uniq<IndexScanLocalState>();
		if (position_data || input.CanRemoveFilterColumns()) {
			l_state->all_columns.Initialize(context.client, scanned_types);
		}
		l_state->scan_state.options.force_fetch_row = Settings::Get<DebugForceFetchRowSetting>(context.client);

		// Initialize the local storage scan.
		auto &bind_data = input.bind_data->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &storage = duck_table.GetStorage();
		auto &local_storage = LocalStorage::Get(context.client, duck_table.catalog);

		l_state->scan_state.Initialize(column_ids, context.client, input.filters.get());
		local_storage.InitializeScan(storage, l_state->scan_state.local_state, input.filters);

		if (position_data) {
			l_state->position_state = make_uniq<PositionScanLocalState>();
			auto &position = *l_state->position_state;
			position.scan_state.Initialize(column_ids, context.client);
			auto &table_state = position.scan_state.table_state;
			table_state.row_groups = position_data->row_groups;
			table_state.Initialize(context.client, position_data->row_group_collection->GetTypes());
		}
		return std::move(l_state);
	}

	void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) override {
		auto &l_state = data_p.local_state->Cast<IndexScanLocalState>();
		D_ASSERT(bool(position_data) == bool(l_state.position_state));
		auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &tx = DuckTransaction::Get(context, duck_table.catalog);
		auto &storage = duck_table.GetStorage();
		const auto persistent_batch_count = position_data
		                                        ? position_data->batches.size()
		                                        : (row_id_count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
		// We might need to loop back, so while (true)
		while (true) {
			if (l_state.in_charge_of_final_stretch) {
				ScanLocalStorage(tx, l_state, output);
				return;
			}
			const auto batch_index = next_batch_index.fetch_add(1);
			if (batch_index > persistent_batch_count) {
				// No work to be picked up
				return;
			}
			l_state.batch_index = batch_index;
			if (batch_index == persistent_batch_count) {
				l_state.in_charge_of_final_stretch = true;
				continue;
			}
			if (position_data) {
				ScanPositionBatch(context, tx, l_state, output);
			} else {
				FetchPersistentBatch(tx, storage, l_state, output);
			}
			if (output.size() == 0) {
				if (data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
					data_p.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
					return;
				}
				continue;
			}
			return;
		}
	}

	double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const override {
		if (position_data) {
			if (position_data->positions.empty()) {
				return 100;
			}
			const auto scanned_rows = position_data->completed_rows.load();
			auto percentage =
			    100 * (static_cast<double>(scanned_rows) / static_cast<double>(position_data->positions.size()));
			return percentage > 100 ? 100 : percentage;
		}
		if (row_id_count == 0) {
			return 100;
		}
		auto scanned_rows = next_batch_index * STANDARD_VECTOR_SIZE;
		auto percentage = 100 * (static_cast<double>(scanned_rows) / static_cast<double>(row_id_count));
		return percentage > 100 ? 100 : percentage;
	}

	OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                TableFunctionGetPartitionInput &input) override {
		auto &l_state = input.local_state->Cast<IndexScanLocalState>();
		return OperatorPartitionData(l_state.batch_index);
	}

	idx_t TableScanRowsScanned(LocalTableFunctionState &state) override {
		auto &l_state = state.Cast<IndexScanLocalState>();
		return l_state.rows_scanned;
	}

	idx_t TableScanRowGroupsScanned(LocalTableFunctionState &) override {
		return 0;
	}

private:
	void FetchPersistentBatch(DuckTransaction &transaction, DataTable &storage, IndexScanLocalState &l_state,
	                          DataChunk &output) {
		const auto offset = l_state.batch_index * STANDARD_VECTOR_SIZE;
		D_ASSERT(offset < row_id_count);
		const auto scan_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, row_id_count - offset);
		auto row_id_data = reinterpret_cast<data_ptr_t>(row_ids + offset);
		Vector local_vector(LogicalType::ROW_TYPE, row_id_data, scan_count);

		if (CanRemoveFilterColumns()) {
			l_state.all_columns.Reset();
			storage.Fetch(transaction, l_state.all_columns, column_ids, local_vector, scan_count, l_state.fetch_state);
			output.ReferenceColumns(l_state.all_columns, projection_ids);
		} else {
			storage.Fetch(transaction, output, column_ids, local_vector, scan_count, l_state.fetch_state);
		}
		l_state.rows_scanned += scan_count;
	}

	void ScanLocalStorage(DuckTransaction &transaction, IndexScanLocalState &l_state, DataChunk &output) {
		auto &local_storage = LocalStorage::Get(transaction);
		if (CanRemoveFilterColumns()) {
			l_state.all_columns.Reset();
			local_storage.Scan(l_state.scan_state.local_state, column_ids, l_state.all_columns);
			output.ReferenceColumns(l_state.all_columns, projection_ids);
		} else {
			local_storage.Scan(l_state.scan_state.local_state, column_ids, output);
		}
		l_state.rows_scanned += output.size();
	}

	void ScanPositionBatch(ClientContext &context, DuckTransaction &transaction, IndexScanLocalState &l_state,
	                       DataChunk &output) {
		auto &position = *l_state.position_state;
		const auto &batch = position_data->batches[l_state.batch_index];
		const auto batch_window_end = batch.window_begin + batch.window_count;
		const bool use_window_chunk = batch.window_count > 1;
		l_state.all_columns.Reset();
		if (use_window_chunk && position.window_chunk.ColumnCount() == 0) {
			position.window_chunk.Initialize(context, scanned_types);
		}
		{
			// Do not hold the revert lock while the caller consumes this batch's output.
			auto row_group_revert_lock =
			    position_data->row_group_collection->GetTableInfo().GetSharedRowGroupRevertLock();
			const auto live_row_end =
			    position_data->row_groups->GetBaseRowId() + position_data->row_group_collection->GetNextRowId();
			for (idx_t window_idx = batch.window_begin; window_idx < batch_window_end; window_idx++) {
				if (window_idx > batch.window_begin) {
					context.InterruptCheck();
				}
				const auto &window = position_data->windows[window_idx];
				auto &scan_chunk = use_window_chunk ? position.window_chunk : l_state.all_columns;
				scan_chunk.Reset();
				ScanPositionWindow(transaction, position.scan_state.table_state, position_data->positions, window,
				                   live_row_end, l_state.fetch_state, scan_chunk);
				if (use_window_chunk) {
					l_state.all_columns.Append(scan_chunk);
				}
			}
		}

		position_data->completed_rows.fetch_add(batch.candidate_count);
		l_state.rows_scanned += batch.candidate_count;
		if (CanRemoveFilterColumns()) {
			output.ReferenceColumns(l_state.all_columns, projection_ids);
		} else {
			output.Reference(l_state.all_columns);
		}
	}
};

class DuckTableScanState : public TableScanGlobalState {
public:
	DuckTableScanState(ClientContext &context, const FunctionData *bind_data_p)
	    : TableScanGlobalState(context, bind_data_p), bind_data(bind_data_p->Cast<TableScanBindData>()),
	      duck_table(bind_data.table.Cast<DuckTableEntry>()), tx(DuckTransaction::Get(context, duck_table.catalog)),
	      storage(duck_table.GetStorage()), total_rows(storage.GetTotalRows()) {
	}

public:
	ParallelTableScanState state;

private:
	const TableScanBindData &bind_data;
	DuckTableEntry &duck_table;
	DuckTransaction &tx;
	DataTable &storage;
	const idx_t total_rows;
	//! Scan initialization info retained for creating scan states
	vector<StorageIndex> storage_ids;
	optional_ptr<TableFilterSet> filters;
	optional_ptr<SampleOptions> sample_options;

public:
	//! Retains the scan initialization info shared by all scan states of this scan
	void InitializeScanInfo(TableFunctionInitInput &input) {
		for (auto &col : input.column_indexes) {
			storage_ids.push_back(bind_data.table.GetStorageIndex(col));
		}
		filters = input.filters;
		sample_options = input.sample_options;
	}

	//! Shared scan-state setup for this table scan
	void InitializeScanState(ClientContext &context, TableScanState &scan_state) const {
		if (bind_data.order_options) {
			scan_state.table_state.reorderer =
			    make_uniq<RowGroupReorderer>(*bind_data.order_options, TransactionData(tx));
			scan_state.local_state.reorderer =
			    make_uniq<RowGroupReorderer>(*bind_data.order_options, TransactionData(tx));
		}
		scan_state.Initialize(storage_ids, context, filters, sample_options, total_rows);
		scan_state.options.force_fetch_row = Settings::Get<DebugForceFetchRowSetting>(context);
	}

	unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                   TableFunctionInitInput &input) override {
		auto l_state = make_uniq<TableScanLocalState>();
		InitializeScanState(context.client, l_state->scan_state);

		l_state->rows_in_current_row_group = storage.NextParallelScan(context.client, state, l_state->scan_state);
		if (l_state->rows_in_current_row_group > 0) {
			l_state->row_groups_scanned++;
		}
		if (input.CanRemoveFilterColumns()) {
			l_state->all_columns.Initialize(context.client, scanned_types);
		}
		return std::move(l_state);
	}

	//! How TableScanFunc's loop proceeds after a persistent scan iteration
	enum class PersistentScanResult { YIELD, NEXT_VECTOR, EXHAUSTED };

	//! Emits a scanned chunk into the output, scanning into all_columns first when filter columns are removed
	template <class FUNC>
	void EmitChunk(TableScanLocalState &l_state, DataChunk &output, FUNC &&scan) {
		if (!CanRemoveFilterColumns()) {
			scan(output);
			return;
		}
		l_state.all_columns.Reset();
		scan(l_state.all_columns);
		output.ReferenceColumns(l_state.all_columns, projection_ids);
	}

	//! Prepares the next vector, schedules its I/O and decodes it, draining local storage when exhausted
	PersistentScanResult ScanPersistentStorage(ClientContext &context, TableFunctionInput &data_p,
	                                           TableScanLocalState &l_state, DataChunk &output) {
		// persistent storage phase, prepare the next vector and schedule its I/O before decoding
		auto &table_state = l_state.scan_state.table_state;
		vector<unique_ptr<AsyncTask>> io_tasks;
		if (!table_state.PrepareScanIO(tx, io_tasks)) {
			// we are done, scan drains any claimed local storage rows
			EmitChunk(l_state, output, [&](DataChunk &chunk) { storage.Scan(tx, chunk, l_state.scan_state); });
			return PersistentScanResult::EXHAUSTED;
		}
		auto io_result = AsyncResult::FromTasks(std::move(io_tasks), TaskSchedulerType::ASYNC);
		// on resume the prepared vector is decoded without registering I/O again
		if (io_result.GetResultType() == AsyncResultType::BLOCKED && data_p.HandleBlocked(io_result)) {
			return PersistentScanResult::YIELD;
		}
		EmitChunk(l_state, output, [&](DataChunk &chunk) { table_state.ProcessPreparedScan(tx, chunk); });
		if (output.size() > 0) {
			return PersistentScanResult::YIELD;
		}
		// the prepared vector was filtered out entirely, go the next vector
		context.InterruptCheck();
		return PersistentScanResult::NEXT_VECTOR;
	}

	void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) override {
		auto &l_state = data_p.local_state->Cast<TableScanLocalState>();
		l_state.scan_state.options.force_fetch_row = Settings::Get<DebugForceFetchRowSetting>(context);

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
		{
			AsyncResult test_result;
			if (AsyncResult::TryGenerateTestResult(test_result) && data_p.HandleBlocked(test_result)) {
				return;
			}
		}
#endif

		do {
			if (bind_data.is_create_index) {
				storage.CreateIndexScan(l_state.scan_state, output);
			} else {
				switch (ScanPersistentStorage(context, data_p, l_state, output)) {
				case PersistentScanResult::YIELD:
					return;
				case PersistentScanResult::NEXT_VECTOR:
					continue;
				case PersistentScanResult::EXHAUSTED:
					break;
				}
			}
			if (output.size() > 0) {
				return;
			}

			l_state.rows_in_current_row_group = storage.NextParallelScan(context, state, l_state.scan_state);
			if (l_state.rows_in_current_row_group > 0) {
				l_state.row_groups_scanned++;
			}

			if (data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				// We can avoid looping, and just return as appropriate
				if (l_state.rows_in_current_row_group == 0) {
					data_p.async_result = AsyncResultType::FINISHED;
				} else {
					data_p.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
				}
				return;
			}
			if (l_state.rows_in_current_row_group == 0) {
				return;
			}

			// Before looping back, check if we are interrupted
			context.InterruptCheck();
		} while (true);
	}

	double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const override {
		// The table is empty or smaller than the standard vector size.
		if (total_rows == 0) {
			return 100;
		}

		idx_t scanned_rows = state.scan_state.processed_rows;
		scanned_rows += state.local_state.processed_rows;
		auto percentage = 100 * (static_cast<double>(scanned_rows) / static_cast<double>(total_rows));
		if (percentage > 100) {
			// If the last chunk has fewer elements than STANDARD_VECTOR_SIZE, and if our percentage is over 100,
			// then we finished this table.
			return 100;
		}
		return percentage;
	}

	OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                TableFunctionGetPartitionInput &input) override {
		auto &l_state = input.local_state->Cast<TableScanLocalState>();
		if (l_state.scan_state.table_state.row_group) {
			return OperatorPartitionData(l_state.scan_state.table_state.batch_index);
		}
		if (l_state.scan_state.local_state.row_group) {
			return OperatorPartitionData(l_state.scan_state.table_state.batch_index +
			                             l_state.scan_state.local_state.batch_index);
		}
		return OperatorPartitionData(0);
	}

	idx_t TableScanRowsScanned(LocalTableFunctionState &state) override {
		const auto &l_state = state.Cast<TableScanLocalState>();
		return l_state.scan_state.table_state.rows_scanned + l_state.scan_state.local_state.rows_scanned;
	}

	idx_t TableScanRowGroupsScanned(LocalTableFunctionState &state) override {
		auto &l_state = state.Cast<TableScanLocalState>();
		return l_state.row_groups_scanned;
	}
};

static unique_ptr<LocalTableFunctionState> TableScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *g_state) {
	auto &cast_g_state = g_state->Cast<TableScanGlobalState>();
	return cast_g_state.InitLocalState(context, input);
}

unique_ptr<GlobalTableFunctionState> DuckTableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input,
                                                             DataTable &storage, const TableScanBindData &bind_data) {
	auto g_state = make_uniq<DuckTableScanState>(context, input.bind_data.get());
	if (bind_data.order_options) {
		auto transaction = TransactionData(DuckTransaction::Get(context, storage.GetAttached()));
		g_state->state.scan_state.reorderer = make_uniq<RowGroupReorderer>(*bind_data.order_options, transaction);
		g_state->state.local_state.reorderer = make_uniq<RowGroupReorderer>(*bind_data.order_options, transaction);
	}
	if (bind_data.partitions_to_scan) {
		g_state->state.scan_state.partitions_to_scan = bind_data.partitions_to_scan.get();
	}

	// Check if row_number column is requested and initialize row_number_base
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		if (input.column_ids[i] == COLUMN_IDENTIFIER_ROW_NUMBER) {
			g_state->state.scan_state.row_number_base = 0;
			break;
		}
	}
	storage.InitializeParallelScan(context, g_state->state, input.column_indexes);
	g_state->InitializeScanInfo(input);
	if (!input.CanRemoveFilterColumns()) {
		return std::move(g_state);
	}

	g_state->projection_ids = input.projection_ids;
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	const auto &columns = duck_table.GetColumns();
	for (const auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn() || col_idx.IsRowNumberColumn()) {
			g_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else if (col_idx.HasType()) {
			g_state->scanned_types.push_back(col_idx.GetScanType());
		} else {
			g_state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}
	return std::move(g_state);
}

unique_ptr<GlobalTableFunctionState> DuckIndexScanInitGlobal(ClientContext &context, TableFunctionInitInput &input,
                                                             const TableScanBindData &bind_data, set<row_t> &row_ids,
                                                             unique_ptr<StorageLockKey> vacuum_lock) {
	auto g_state = make_uniq<DuckIndexScanState>(context, input.bind_data.get());
	g_state->vacuum_lock = std::move(vacuum_lock);

	const auto can_use_position_scan = !row_ids.empty() && !Settings::Get<DebugForceFetchRowSetting>(context);

	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	if (input.CanRemoveFilterColumns()) {
		g_state->projection_ids = input.projection_ids;
	}

	const auto &columns = duck_table.GetColumns();
	for (const auto &col_idx : input.column_indexes) {
		g_state->column_ids.push_back(bind_data.table.GetStorageIndex(col_idx));
		if (col_idx.IsRowIdColumn()) {
			g_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else if (col_idx.HasType()) {
			g_state->scanned_types.emplace_back(col_idx.GetScanType());
		} else {
			g_state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}

	// Const-cast to indicate an index scan.
	// We need this information in the bind data so that we can access it during ANALYZE.
	auto &no_const_bind_data = bind_data.CastNoConst<TableScanBindData>();
	no_const_bind_data.is_index_scan = true;

	if (can_use_position_scan) {
		auto position_data = make_uniq<PositionScanData>();
		// ART entry locks are released by TryScanIndex before this point. Acquiring the shared revert lock here
		// avoids a lock-order cycle with rollback, which removes index entries before truncating storage.
		position_data->row_group_collection = duck_table.GetStorage().GetRowGroupCollection();
		auto row_group_revert_lock = position_data->row_group_collection->GetTableInfo().GetSharedRowGroupRevertLock();
		position_data->row_groups = position_data->row_group_collection->GetRowGroups();
		position_data->positions.assign(row_ids.begin(), row_ids.end());
		BuildPositionScanPlan(*position_data->row_groups, position_data->positions, position_data->windows,
		                      position_data->batches);
		g_state->max_threads = MinValue(g_state->max_threads, position_data->batches.size() + 1);
		g_state->position_data = std::move(position_data);
	}

	// Keep the original ART row IDs for the existing Fetch path, including IDs that were not present in the pinned tree
	// and transaction-local IDs.
	if (!row_ids.empty() && !g_state->position_data) {
		auto row_id_ptr = g_state->arena.AllocateAligned(row_ids.size() * sizeof(row_t));
		g_state->row_ids = reinterpret_cast<row_t *>(row_id_ptr);
		g_state->row_id_count = row_ids.size();

		idx_t row_id_count = 0;
		for (const auto row_id : row_ids) {
			g_state->row_ids[row_id_count++] = row_id;
		}
	}
	return std::move(g_state);
}

struct ComparisonCondition {
	ExpressionType type;
	Value constant;
};

static bool CollectValuesAndComparisonsFromExpression(const Expression &expr, value_set_t &in_values,
                                                      vector<ComparisonCondition> &comparisons) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::COMPARE_IN) {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.GetChildren().empty() || op.GetChildren()[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
			return false;
		}
		for (idx_t i = 1; i < op.GetChildren().size(); i++) {
			if (op.GetChildren()[i]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
				return false;
			}
			auto &value = op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue();
			if (!value.IsNull()) {
				in_values.insert(value);
			}
		}
		return true;
	}
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comp = expr.Cast<BoundFunctionExpression>();
		Value val;
		auto &left = BoundComparisonExpression::Left(comp);
		auto &right = BoundComparisonExpression::Right(comp);
		bool left_is_ref = left.GetExpressionClass() == ExpressionClass::BOUND_REF;
		bool right_is_ref = right.GetExpressionClass() == ExpressionClass::BOUND_REF;
		if (right.GetExpressionType() == ExpressionType::VALUE_CONSTANT && left_is_ref) {
			val = right.Cast<BoundConstantExpression>().GetValue();
		} else if (left.GetExpressionType() == ExpressionType::VALUE_CONSTANT && right_is_ref) {
			val = left.Cast<BoundConstantExpression>().GetValue();
		} else {
			return false;
		}
		if (val.IsNull()) {
			return false;
		}
		if (comp.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
			in_values.insert(val);
		}
		comparisons.push_back({comp.GetExpressionType(), std::move(val)});
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj.GetChildren()) {
			if (!CollectValuesAndComparisonsFromExpression(*child, in_values, comparisons)) {
				return false;
			}
		}
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME) {
			if (!func.BindInfo()) {
				return true;
			}
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return !data.child_filter_expr ||
			       CollectValuesAndComparisonsFromExpression(*data.child_filter_expr, in_values, comparisons);
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME) {
			if (!func.BindInfo()) {
				return true;
			}
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return !data.child_filter_expr ||
			       CollectValuesAndComparisonsFromExpression(*data.child_filter_expr, in_values, comparisons);
		}
		if (TableFilterFunctions::IsTableFilterFunction(func.Function())) {
			return true;
		}
	}
	return false;
}

//! Check if a value qualifies against all extracted comparison conditions.
static bool ValueQualifies(const Value &value, const vector<ComparisonCondition> &comparisons) {
	for (auto &comp : comparisons) {
		bool passes;
		switch (comp.type) {
		case ExpressionType::COMPARE_EQUAL:
			passes = ValueOperations::Equals(value, comp.constant);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			passes = ValueOperations::NotEquals(value, comp.constant);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			passes = ValueOperations::GreaterThan(value, comp.constant);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			passes = ValueOperations::GreaterThanEquals(value, comp.constant);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			passes = ValueOperations::LessThan(value, comp.constant);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			passes = ValueOperations::LessThanEquals(value, comp.constant);
			break;
		default:
			return true;
		}
		if (!passes) {
			return false;
		}
	}
	return true;
}

static bool ExtractValuesFromExpression(const Expression &expr, value_set_t &values) {
	value_set_t in_values;
	vector<ComparisonCondition> comparisons;
	if (!CollectValuesAndComparisonsFromExpression(expr, in_values, comparisons) || in_values.empty()) {
		return false;
	}
	for (auto &value : in_values) {
		if (ValueQualifies(value, comparisons)) {
			values.insert(value);
		}
	}
	return !values.empty();
}

void ExtractExpressionsFromValues(const value_set_t &unique_values, BoundColumnRefExpression &bound_ref,
                                  vector<unique_ptr<Expression>> &expressions) {
	for (const auto &value : unique_values) {
		auto bound_constant = make_uniq<BoundConstantExpression>(value);
		auto filter_expr = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, bound_ref.Copy(),
		                                                     std::move(bound_constant));
		expressions.push_back(std::move(filter_expr));
	}
}

vector<unique_ptr<Expression>> ExtractFilterExpressions(const ColumnDefinition &col, const TableFilter &filter,
                                                        idx_t storage_idx) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "ExtractFilterExpressions");
	ColumnBinding binding(TableIndex(0), ProjectionIndex(storage_idx));
	auto bound_ref = make_uniq<BoundColumnRefExpression>(col.Name(), col.Type(), binding);

	// Extract all exact values we can derive from the filter tree.
	vector<unique_ptr<Expression>> expressions;
	value_set_t values;
	if (ExtractValuesFromExpression(*expr_filter.expr, values)) {
		ExtractExpressionsFromValues(values, *bound_ref, expressions);
	}

	// Attempt matching the top-level filter to the index expression.
	if (expressions.empty()) {
		auto filter_expr = expr_filter.ToExpression(*bound_ref);
		expressions.push_back(std::move(filter_expr));
	}

	return expressions;
}

bool TryScanIndex(const IndexReadHandle<ART> &art, const ColumnList &column_list, TableFunctionInitInput &input,
                  TableFilterSet &filter_set, idx_t max_count, set<row_t> &row_ids) {
	// FIXME: No support for index scans on compound ARTs.
	// See note above on multi-filter support.
	if (art->UnboundExpressionCount() > 1) {
		return false;
	}

	auto index_expr = art->CopyUnboundExpression(0);
	auto indexed_columns = art->GetColumnIds();

	// NOTE: We do not push down multi-column filters, e.g., 42 = a + b.
	if (indexed_columns.size() != 1) {
		return false;
	}

	// Resolve bound column references in the index_expr against the current input projection
	ProjectionIndex updated_index_column;
	bool found_index_column_in_input = false;

	// Find the indexed column amongst the input columns
	for (idx_t i = 0; i < input.column_ids.size(); ++i) {
		if (input.column_ids[i] == indexed_columns[0]) {
			updated_index_column = ProjectionIndex(i);
			found_index_column_in_input = true;
			break;
		}
	}

	// If found, update the bound column ref within index_expr
	if (found_index_column_in_input) {
		ExpressionIterator::EnumerateExpression(index_expr, [&](Expression &expr) {
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return;
			}

			auto &bound_column_ref_expr = expr.Cast<BoundColumnRefExpression>();

			// If the bound column references the index column, use updated_index_column
			if (bound_column_ref_expr.Binding().column_index == indexed_columns[0]) {
				bound_column_ref_expr.BindingMutable().column_index = updated_index_column;
			}
		});
	}

	// Get ART column.
	auto &col = column_list.GetColumn(LogicalIndex(indexed_columns[0]));

	// The indexes of the filters match input.column_indexes, which are: i -> column_index.
	// Try to find a filter on the ART column.
	ProjectionIndex storage_index;
	for (idx_t i = 0; i < input.column_indexes.size(); i++) {
		if (input.column_indexes[i].ToLogical() == col.Logical()) {
			storage_index = ProjectionIndex(i);
			break;
		}
	}

	// No filter matches the ART column.
	if (!storage_index.IsValid()) {
		return false;
	}

	// Try to find a matching filter for the column.
	auto filter = filter_set.TryGetFilterByColumnIndex(storage_index);
	if (!filter) {
		return false;
	}

	auto expressions = ExtractFilterExpressions(col, *filter, storage_index.GetIndex());
	for (const auto &filter_expr : expressions) {
		auto scan_state = art->TryInitializeScan(*index_expr, *filter_expr);
		if (!scan_state) {
			return false;
		}

		if (!art->Scan(*scan_state, max_count, row_ids)) {
			row_ids.clear();
			return false;
		}
		for (const auto delta : {IndexDeltaType::DELETED_ROWS_IN_USE, IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT}) {
			auto delta_index = art.FindDelta(delta);
			if (!delta_index) {
				continue;
			}
			auto delta_scan_state = delta_index->TryInitializeScan(*index_expr, *filter_expr);
			if (!delta_scan_state) {
				return false;
			}

			// Check if we can use an index scan, and already retrieve the matching row ids.
			if (!delta_index->Scan(*delta_scan_state, max_count, row_ids)) {
				row_ids.clear();
				return false;
			}
		}
	}
	return true;
}

unique_ptr<GlobalTableFunctionState> TableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	D_ASSERT(input.bind_data);

	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();

	// Can't index scan without filters.
	if (!input.filters) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	// Row-number materialization relies on the ordinary table scan's physical-order state.
	for (const auto &column : input.column_indexes) {
		if (column.IsRowNumberColumn()) {
			return DuckTableScanInitGlobal(context, input, storage, bind_data);
		}
	}

	// Only scan specific partitions
	if (bind_data.partitions_to_scan) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	auto &filter_set = *input.filters;

	// FIXME: We currently only support scanning one ART with one filter.
	// If multiple filters exist, i.e., a = 11 AND b = 24, we need to
	// 1.	1.1. Find + scan one ART for a = 11.
	//		1.2. Find + scan one ART for b = 24.
	//		1.3. Return the intersecting row IDs.
	// 2. (Reorder and) scan a single ART with a compound key of (a, b).
	if (filter_set.FilterCount() != 1) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	auto &info = storage.GetDataTableInfo();
	auto &indexes = info->GetIndexes();
	if (indexes.Empty()) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	auto scan_percentage = Settings::Get<IndexScanPercentageSetting>(context);
	auto scan_max_count = Settings::Get<IndexScanMaxCountSetting>(context);

	auto total_rows = storage.GetTotalRows();
	auto total_rows_from_percentage = LossyNumericCast<idx_t>(double(total_rows) * scan_percentage);
	auto max_count = MaxValue(scan_max_count, total_rows_from_percentage);

	auto &column_list = duck_table.GetColumns();
	bool index_scan = false;
	set<row_t> row_ids;

	info->BindIndexes(context, ART::TYPE_NAME);

	// Exclude rowid-shifting vacuum from the ART probe until the index scan finishes: collected rowids must be
	// fetched against the matching row-group tree. Falling back to a table scan releases the lock on return.
	unique_ptr<StorageLockKey> vacuum_lock;
	auto &attached = storage.GetAttached();
	const bool indexed_vacuum_may_move_rowids = attached.GetVacuumRebuildIndexThreshold() > 0 ||
	                                            StorageCompatibility::FromDatabase(attached).CanPersistRowIdGaps();
	if (indexed_vacuum_may_move_rowids) {
		vacuum_lock = DuckTransactionManager::Get(attached).SharedVacuumLock();
	}

	for (auto entry : indexes.IndexEntries()) {
		if (entry->GetBindState() != IndexBindState::BOUND || entry->GetIndexType() != ART::TYPE_NAME) {
			continue;
		}
		auto index = entry->GetReadHandle<ART>();
		index_scan = TryScanIndex(index, column_list, input, filter_set, max_count, row_ids);
		if (index_scan) {
			// found an index - break
			break;
		}
	}

	if (!index_scan) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}
	return DuckIndexScanInitGlobal(context, input, bind_data, row_ids, std::move(vacuum_lock));
}

static unique_ptr<BaseStatistics> TableScanStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input) {
	auto &column_id = input.column_index;
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &local_storage = LocalStorage::Get(context, duck_table.catalog);

	// Don't emit statistics for tables with outstanding transaction-local data.
	if (local_storage.Find(duck_table.GetStorage())) {
		return nullptr;
	}

	if (column_id.IsRowIdColumn() || column_id.IsRowNumberColumn()) {
		return nullptr;
	}
	auto &column = duck_table.GetColumn(LogicalIndex(column_id.GetPrimaryIndex()));
	if (column.Generated()) {
		return nullptr;
	}

	auto storage_index = duck_table.GetStorageIndex(column_id);
	return duck_table.GetStatistics(context, storage_index);
}

static void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &g_state = data_p.global_state->Cast<TableScanGlobalState>();
	g_state.TableScanFunc(context, data_p, output);
}

double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *g_state_p) {
	auto &g_state = g_state_p->Cast<TableScanGlobalState>();
	return g_state.TableScanProgress(context, bind_data_p);
}

OperatorPartitionData TableScanGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("TableScan::GetPartitionData: partition columns not supported");
	}

	auto &g_state = input.global_state->Cast<TableScanGlobalState>();
	return g_state.TableScanGetPartitionData(context, input);
}

vector<PartitionStatistics> TableScanGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input) {
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();
	return storage.GetPartitionStats(context);
}

BindInfo TableScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	return BindInfo(bind_data.table);
}

void TableScanDependency(LogicalDependencyList &entries, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	entries.AddDependency(bind_data.table);
}

unique_ptr<NodeStatistics> TableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &local_storage = LocalStorage::Get(context, duck_table.catalog);
	auto &storage = duck_table.GetStorage();
	idx_t table_rows = storage.GetTotalRows();
	idx_t estimated_cardinality = table_rows + local_storage.AddedRows(duck_table.GetStorage());
	return make_uniq<NodeStatistics>(estimated_cardinality, estimated_cardinality);
}

void TableScanGetMetrics(TableFunctionGetMetricsInput &input) {
	auto &gstate = input.global_state->Cast<TableScanGlobalState>();
	auto &local_state = *input.local_state;
	input.operator_metrics.rows_scanned = gstate.TableScanRowsScanned(local_state);
	input.operator_metrics.row_groups_scanned = gstate.TableScanRowGroupsScanned(local_state);
	input.operator_metrics.total_row_groups_to_scan = gstate.total_row_groups_to_scan;
}

InsertionOrderPreservingMap<string> TableScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	result["Table"] = bind_data.table.schema.GetQualifiedName(bind_data.table.name)
	                      .ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
	result["Type"] = bind_data.is_index_scan ? "Index Scan" : "Sequential Scan";
	return result;
}

static void TableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	// the catalog/schema/name are only the innermost qualification - "qualified_name" carries the full (possibly
	// nested) schema path
	serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
	serializer.WriteProperty(102, "table", bind_data.table.name);
	serializer.WriteProperty(103, "is_index_scan", bind_data.is_index_scan);
	serializer.WriteProperty(104, "is_create_index", bind_data.is_create_index);
	serializer.WritePropertyWithDefault(105, "result_ids", unsafe_vector<row_t>());
	serializer.WritePropertyWithDefault<QualifiedName>(
	    106, "qualified_name", bind_data.table.schema.GetQualifiedName(bind_data.table.name), QualifiedName());
}

static unique_ptr<FunctionData> TableScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto catalog = deserializer.ReadProperty<Identifier>(100, "catalog");
	auto schema = deserializer.ReadProperty<Identifier>(101, "schema");
	auto table = deserializer.ReadProperty<Identifier>(102, "table");
	auto is_index_scan = deserializer.ReadProperty<bool>(103, "is_index_scan");
	auto is_create_index = deserializer.ReadProperty<bool>(104, "is_create_index");
	deserializer.ReadDeletedProperty<unsafe_vector<row_t>>(105, "result_ids");
	// plans written before nested schema support only have the innermost qualification
	auto qualified_name =
	    deserializer.ReadPropertyWithExplicitDefault<QualifiedName>(106, "qualified_name", QualifiedName());
	if (qualified_name.Path().empty()) {
		qualified_name = QualifiedName(catalog, schema, table);
	}
	auto &catalog_entry = Catalog::GetEntry<TableCatalogEntry>(deserializer.Get<ClientContext &>(), qualified_name);
	if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
		throw SerializationException("Cant find table for %s.%s", schema, table);
	}
	auto result = make_uniq<TableScanBindData>(catalog_entry.Cast<DuckTableEntry>());
	result->is_index_scan = is_index_scan;
	result->is_create_index = is_create_index;
	return std::move(result);
}

static bool TableSupportsPushdownExtract(const FunctionData &bind_data_ref, const LogicalIndex &column_idx) {
	auto &bind_data = bind_data_ref.Cast<TableScanBindData>();
	auto &column = bind_data.table.GetColumn(column_idx);
	if (column.Generated()) {
		return false;
	}
	auto column_type = column.GetType();
	return column_type.id() == LogicalTypeId::STRUCT || column_type.id() == LogicalTypeId::VARIANT;
}

bool TableScanPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	return true;
}

virtual_column_map_t TableScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	return bind_data.table.GetVirtualColumns();
}

vector<column_t> TableScanGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	vector<column_t> result;
	result.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	return result;
}

void SetScanOrder(unique_ptr<RowGroupOrderOptions> order_options, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	bind_data.order_options = std::move(order_options);
}

void SetPartitionsToScan(vector<idx_t> partition_indices, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	bind_data.partitions_to_scan = make_uniq<unordered_set<idx_t>>(partition_indices.begin(), partition_indices.end());
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, TableScanFunc);
	scan_function.init_local = TableScanInitLocal;
	scan_function.init_global = TableScanInitGlobal;
	scan_function.statistics_extended = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.get_metrics = TableScanGetMetrics;
	scan_function.pushdown_complex_filter = nullptr;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = TableScanProgress;
	scan_function.get_partition_data = TableScanGetPartitionData;
	scan_function.get_partition_stats = TableScanGetPartitionStats;
	scan_function.get_bind_info = TableScanGetBindInfo;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	scan_function.filter_prune = true;
	scan_function.sampling_pushdown = true;
	scan_function.late_materialization = true;
	scan_function.serialize = TableScanSerialize;
	scan_function.deserialize = TableScanDeserialize;
	scan_function.pushdown_expression = TableScanPushdownExpression;
	scan_function.get_virtual_columns = TableScanGetVirtualColumns;
	scan_function.get_row_id_columns = TableScanGetRowIdColumns;
	scan_function.set_scan_order = SetScanOrder;
	scan_function.set_partitions_to_scan = SetPartitionsToScan;
	scan_function.supports_pushdown_extract = TableSupportsPushdownExtract;
	return scan_function;
}

void TableScanFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet table_scan_set("seq_scan");
	table_scan_set.AddFunction(GetFunction());
	set.AddFunction(std::move(table_scan_set));
}

void BuiltinFunctions::RegisterTableScanFunctions() {
	TableScanFunction::RegisterFunction(*this);
}

} // namespace duckdb
