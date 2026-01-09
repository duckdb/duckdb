#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/execution/operator/schema/physical_create_index_materialized.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

PhysicalCreateIndexMaterialized::PhysicalCreateIndexMaterialized(
    PhysicalPlan &physical_plan, const vector<LogicalType> &types_p, TableCatalogEntry &table_p,
    const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
    vector<unique_ptr<Expression>> unbound_expressions, idx_t estimated_cardinality)
    // Declare this operators as a EXTENSION operator
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types_p, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)), unbound_expressions(std::move(unbound_expressions)),
      sorted(false) {
	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//-------------------------------------------------------------
// Global State
//-------------------------------------------------------------
class CreateMaterializedIndexGlobalState final : public GlobalSinkState {
public:
	CreateMaterializedIndexGlobalState(const PhysicalOperator &op_p) : op(op_p) {
	}

	const PhysicalOperator &op;
	//! Global index to be added to the table
	// this was hnsw index?
	unique_ptr<BoundIndex> global_index;

	mutex glock;
	unique_ptr<ColumnDataCollection> collection;
	shared_ptr<ClientContext> context;

	// Parallel scan state
	ColumnDataParallelScanState scan_state;

	// Track which phase we're in
	atomic<bool> is_building = {false};
	atomic<idx_t> loaded_count = {0};
	atomic<idx_t> built_count = {0};
};

unique_ptr<BoundIndex> PhysicalCreateIndexMaterialized::CreateMaterializedIndex() const {
	// or template? do something
	return make_uniq<MaterializedIndex>(info->index_name, constraint_type, storage_ids, table_manager,
	                                    unbound_expressions, db, info->options, IndexStorageInfo(),
	                                    estimated_cardinality);
}

unique_ptr<GlobalSinkState> PhysicalCreateIndexMaterialized::GetGlobalSinkState(ClientContext &context) const {
	auto gstate = make_uniq<CreateMaterializedIndexGlobalState>(*this);

	vector<LogicalType> data_types = {unbound_expressions[0]->return_type, LogicalType::ROW_TYPE};
	gstate->collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), data_types);
	gstate->context = context.shared_from_this();

	// Create the index
	auto &storage = table.GetStorage();
	auto &table_manager = TableIOManager::Get(storage);
	auto &constraint_type = info->constraint_type;
	auto &db = storage.db;

	// BoundIndex is abstract
	// Maybe input an IndexType?
	gstate->global_index = CreateMaterializedIndex();
	return std::move(gstate);
}

//-------------------------------------------------------------
// Local State
//-------------------------------------------------------------
class CreateMaterializedIndexLocalState final : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

unique_ptr<LocalSinkState> PhysicalCreateIndexMaterialized::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CreateMaterializedIndexLocalState>();

	vector<LogicalType> data_types = {unbound_expressions[0]->return_type, LogicalType::ROW_TYPE};
	state->collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context.client), data_types);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

//-------------------------------------------------------------
// Sink
//-------------------------------------------------------------

SinkResultType PhysicalCreateIndexMaterialized::Sink(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<CreateMaterializedIndexLocalState>();
	auto &gstate = input.global_state.Cast<CreateMaterializedIndexGlobalState>();
	lstate.collection->Append(lstate.append_state, chunk);
	gstate.loaded_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//-------------------------------------------------------------
// Combine
//-------------------------------------------------------------
SinkCombineResultType PhysicalCreateIndexMaterialized::Combine(ExecutionContext &context,
                                                               OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateMaterializedIndexGlobalState>();
	auto &lstate = input.local_state.Cast<CreateMaterializedIndexLocalState>();

	if (lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = std::move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}

	return SinkCombineResultType::FINISHED;
}

//-------------------------------------------------------------
// Finalize
//-------------------------------------------------------------

class MaterializedIndexConstructTask final : public ExecutorTask {
public:
	MaterializedIndexConstructTask(shared_ptr<Event> event_p, ClientContext &context,
	                               CreateMaterializedIndexGlobalState &gstate_p, size_t thread_id_p,
	                               const PhysicalCreateIndexMaterialized &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), gstate(gstate_p), thread_id(thread_id_p),
	      local_scan_state() {
		// Initialize the scan chunk
		gstate.collection->InitializeScanChunk(scan_chunk);
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		auto &index = gstate.global_index->index;
		auto &scan_state = gstate.scan_state;
		auto &collection = gstate.collection;

		const auto array_size = ArrayType::GetSize(scan_chunk.data[0].GetType());

		while (collection->Scan(scan_state, local_scan_state, scan_chunk)) {
			const auto count = scan_chunk.size();
			auto &vec_vec = scan_chunk.data[0];
			auto &data_vec = ArrayVector::GetEntry(vec_vec);
			auto &rowid_vec = scan_chunk.data[1];

			UnifiedVectorFormat vec_format;
			UnifiedVectorFormat data_format;
			UnifiedVectorFormat rowid_format;

			vec_vec.ToUnifiedFormat(count, vec_format);
			data_vec.ToUnifiedFormat(count * array_size, data_format);
			rowid_vec.ToUnifiedFormat(count, rowid_format);

			const auto row_ptr = UnifiedVectorFormat::GetData<row_t>(rowid_format);
			const auto data_ptr = UnifiedVectorFormat::GetData<float>(data_format);

			for (idx_t i = 0; i < count; i++) {
				const auto vec_idx = vec_format.sel->get_index(i);
				const auto row_idx = rowid_format.sel->get_index(i);

				// Check for NULL values
				const auto vec_valid = vec_format.validity.RowIsValid(vec_idx);
				const auto rowid_valid = rowid_format.validity.RowIsValid(row_idx);
				if (!vec_valid || !rowid_valid) {
					executor.PushError(ErrorData(
					    "Invalid data in Materialized index construction: Cannot construct index with NULL values."));
					return TaskExecutionResult::TASK_ERROR;
				}

				// Add the vector to the index
				const auto result = index.add(row_ptr[row_idx], data_ptr + (vec_idx * array_size), thread_id);

				// Check for errors
				if (!result) {
					executor.PushError(ErrorData(result.error.what()));
					return TaskExecutionResult::TASK_ERROR;
				}
			}

			// Update the built count
			gstate.built_count += count;

			if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
				// yield!
				return TaskExecutionResult::TASK_NOT_FINISHED;
			}
		}

		// Finish task!
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	CreateMaterializedIndexGlobalState &gstate;
	size_t thread_id;

	DataChunk scan_chunk;
	ColumnDataLocalScanState local_scan_state;
};

class MaterializedIndexConstructionEvent final : public BasePipelineEvent {
public:
	MaterializedIndexConstructionEvent(const PhysicalCreateIndexMaterialized &op_p,
	                                   CreateMaterializedIndexGlobalState &gstate_p, Pipeline &pipeline_p,
	                                   CreateIndexInfo &info_p, const vector<column_t> &storage_ids_p,
	                                   DuckTableEntry &table_p)
	    : BasePipelineEvent(pipeline_p), op(op_p), gstate(gstate_p), info(info_p), storage_ids(storage_ids_p),
	      table(table_p) {
	}

	const PhysicalCreateIndexMaterialized &op;
	CreateMaterializedIndexGlobalState &gstate;
	CreateIndexInfo &info;
	const vector<column_t> &storage_ids;
	DuckTableEntry &table;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		// Schedule tasks equal to the number of threads, which will construct the index
		auto &ts = TaskScheduler::GetScheduler(context);
		const auto num_threads = NumericCast<size_t>(ts.NumberOfThreads());

		vector<shared_ptr<Task>> construct_tasks;
		for (size_t tnum = 0; tnum < num_threads; tnum++) {
			construct_tasks.push_back(
			    make_uniq<MaterializedIndexConstructTask>(shared_from_this(), context, gstate, tnum, op));
		}
		SetTasks(std::move(construct_tasks));
	}

	void FinishEvent() override {
		// Mark the index as dirty, update its count
		gstate.global_index->SetDirty();
		gstate.global_index->SyncSize();

		auto &storage = table.GetStorage();

		// If not in memory, persist the index to disk
		if (!storage.db.GetStorageManager().InMemory()) {
			// Finalize the index
			gstate.global_index->PersistToDisk();
		}

		if (!storage.IsRoot()) {
			throw TransactionException("Cannot create index on non-root transaction");
		}

		// Create the index entry in the catalog
		auto &schema = table.schema;
		info.column_ids = storage_ids;

		if (schema.GetEntry(schema.GetCatalogTransaction(*gstate.context), CatalogType::INDEX_ENTRY, info.index_name)) {
			if (info.on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
				throw CatalogException("Index with name \"%s\" already exists", info.index_name);
			}
		}

		const auto index_entry = schema.CreateIndex(schema.GetCatalogTransaction(*gstate.context), info, table).get();
		D_ASSERT(index_entry);
		auto &duck_index = index_entry->Cast<DuckIndexEntry>();
		duck_index.initial_index_size = gstate.global_index->Cast<BoundIndex>().GetInMemorySize();

		// Finally add it to storage
		storage.AddIndex(std::move(gstate.global_index));
	}
};

SinkFinalizeType PhysicalCreateIndexMaterialized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                           OperatorSinkFinalizeInput &input) const {
	// Get the global collection we've been appending to
	auto &gstate = input.global_state.Cast<CreateMaterializedIndexGlobalState>();
	auto &collection = gstate.collection;

	// Move on to the next phase
	gstate.is_building = true;

	// Reserve the index size
	auto &ts = TaskScheduler::GetScheduler(context);
	auto &index = gstate.global_index->index;
	index.reserve({static_cast<size_t>(collection->Count()), static_cast<size_t>(ts.NumberOfThreads())});

	// Initialize a parallel scan for the index construction
	collection->InitializeScan(gstate.scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);

	// Create a new event that will construct the index
	auto new_event =
	    make_shared_ptr<MaterializedIndexConstructionEvent>(*this, gstate, pipeline, *info, storage_ids, table);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

ProgressData PhysicalCreateIndexMaterialized::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
                                                              ProgressData source_progress) const {
	// The "source_progress" is not relevant for CREATE INDEX statements
	ProgressData res;

	const auto &state = gstate.Cast<CreateMaterializedIndexGlobalState>();
	// First half of the progress is appending to the collection
	if (!state.is_building) {
		res.done = state.loaded_count + 0.0;
		res.total = estimated_cardinality + estimated_cardinality;
	} else {
		res.done = state.loaded_count + state.built_count;
		res.total = state.loaded_count + state.loaded_count;
	}
	return res;
}

} // namespace duckdb
