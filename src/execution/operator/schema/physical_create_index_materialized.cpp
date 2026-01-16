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
      sorted(false), needs_count(true), in_parallel(true) {
	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//-------------------------------------------------------------
// Global sink State
//-------------------------------------------------------------
class CreateMaterializedIndexGlobalSinkState final : public GlobalSinkState {
public:
	// shared state
	CreateMaterializedIndexGlobalSinkState(const PhysicalOperator &op_p) : op(op_p) {
	}

	unique_ptr<IndexBuildGlobalState> gstate;
	const PhysicalOperator &op;

	//! Global index to be added to the table
	unique_ptr<BoundIndex> global_index;

	//
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

//---------------------------------------------------------------------------------------------------------------------
// Global Sink
//---------------------------------------------------------------------------------------------------------------------

unique_ptr<GlobalSinkState> PhysicalCreateIndexMaterialized::GetGlobalSinkState(ClientContext &context) const {
	// create global state index
	auto gstate = make_uniq<CreateMaterializedIndexGlobalSinkState>(*this);

	IndexBuildInitWorkerInput global_state_input {bind_data.get(),     context,    table, *info,
	                                              unbound_expressions, storage_ids};

	gstate->gstate = index_type.build_global_init(global_state_input);

	// Question: what to do with this?
	vector<LogicalType> data_types = {unbound_expressions[0]->return_type, LogicalType::ROW_TYPE};
	gstate->collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), data_types);

	gstate->context = context.shared_from_this();

	// Create the index
	return std::move(gstate);
}

//-------------------------------------------------------------
// Local Sink
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
	auto &gstate = input.global_state.Cast<CreateMaterializedIndexGlobalSinkState>();

	// maybe change to need materialize
	if (needs_count) {
		//
		lstate.collection->Append(lstate.append_state, chunk);
		gstate.loaded_count += chunk.size();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//-------------------------------------------------------------
// Combine
//-------------------------------------------------------------
SinkCombineResultType PhysicalCreateIndexMaterialized::Combine(ExecutionContext &context,
                                                               OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateMaterializedIndexGlobalSinkState>();
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

class IndexConstructTask final : public ExecutorTask {
public:
	IndexConstructTask(shared_ptr<Event> event_p, ClientContext &context,
	                   CreateMaterializedIndexGlobalSinkState &gstate_p, size_t thread_id_p,
	                   const PhysicalCreateIndexMaterialized &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), gstate(gstate_p), thread_id(thread_id_p),
	      op_materialized(op_p), local_scan_state() {
		// construct a task?
		// Question: is this ok
		// Do
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// TODO // what to do with this?
		// I think we should abstract this
		// if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
		// 	return TaskExecutionResult::TASK_NOT_FINISHED;
		// }

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	CreateMaterializedIndexGlobalSinkState &gstate;
	size_t thread_id;

	DataChunk scan_chunk;
	ColumnDataLocalScanState local_scan_state;
	const PhysicalCreateIndexMaterialized &op_materialized;
};

struct IndexConstructionEventInput {
	IndexConstructionEventInput(const PhysicalCreateIndexMaterialized &op,
	                            CreateMaterializedIndexGlobalSinkState &gstate, Pipeline &pipeline,
	                            CreateIndexInfo &info, const vector<column_t> &storage_ids, DuckTableEntry &table)
	    : op(op), gstate(gstate), pipeline(pipeline), info(info), storage_ids(storage_ids), table(table) {
	}

	const PhysicalCreateIndexMaterialized &op;
	CreateMaterializedIndexGlobalSinkState &gstate;
	Pipeline &pipeline;
	CreateIndexInfo &info;
	const vector<column_t> &storage_ids;
	DuckTableEntry &table;
};

class IndexConstructionEvent : public BasePipelineEvent {
public:
	explicit IndexConstructionEvent(const IndexConstructionEventInput &input)
	    : BasePipelineEvent(input.pipeline), op(input.op), gstate(input.gstate), info(input.info),
	      storage_ids(input.storage_ids), table(input.table), op_p(input.op) {
	}

	const PhysicalCreateIndexMaterialized &op;
	CreateMaterializedIndexGlobalSinkState &gstate;
	CreateIndexInfo &info;
	const vector<column_t> &storage_ids;
	DuckTableEntry &table;
	const PhysicalCreateIndexMaterialized &op_p;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();
		vector<shared_ptr<Task>> tasks;
		idx_t num_tasks = 1;

		// TODO what should this one be
		if (op.index_type.build_work_init) {
			// We only schedule 1 task
			// Schedule tasks equal to the number of threads
			auto &ts = TaskScheduler::GetScheduler(context);
			num_tasks = NumericCast<size_t>(ts.NumberOfThreads());
		}

		for (size_t tnum = 0; tnum < num_tasks; tnum++) {
			tasks.push_back(make_uniq<IndexConstructTask>(shared_from_this(), context, gstate, tnum, op));
		}

		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		// do a callback, and then call
		op.FinalExecute()
	}
};

// TODO; Implement a callback for the sort
// And a callback for the exact count

SinkFinalizeType PhysicalCreateIndexMaterialized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                           OperatorSinkFinalizeInput &input) const {
	// Get the global collection we've been appending to
	auto &gstate = input.global_state.Cast<CreateMaterializedIndexGlobalSinkState>();

	// Finalize the index
	IndexBuildFinalizeInput finalize_input(*gstate.gstate);
	auto &collection = gstate.collection;

	// the collection contains the exact count
	// This does not make sense
	if (needs_count) {
		finalize_input.has_count = true;
		finalize_input.exact_count = collection->Count();
	}

	// Initialize a parallel scan for the index construction
	if (in_parallel) {
		if (!index_type.build_exec_task) {
			throw TransactionException("'in_parallel' is set, but 'build_exec_task' callback is missing");
		}

		collection->InitializeScan(gstate.scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);

		// Create a new event that will construct the index
		auto index_construction_event_input =
		    IndexConstructionEventInput(*this, gstate, pipeline, *info, storage_ids, table);
		auto new_event = make_shared_ptr<IndexConstructionEvent>(index_construction_event_input);
		event.InsertEvent(std::move(new_event));

	} else {
	}

	// Add the index to the storage.
	storage.AddIndex(std::move(bound_index));
}

return SinkFinalizeType::READY;
}

} // namespace duckdb
