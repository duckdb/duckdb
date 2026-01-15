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
// Global sink State
//-------------------------------------------------------------
class CreateMaterializedIndexGlobalSinkState final : public GlobalSinkState {
public:
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

	IndexBuildInitGlobalStateInput global_state_input {bind_data.get(),     context,    table, *info,
	                                                   unbound_expressions, storage_ids};

	gstate->gstate = index_type.build_global_init(global_state_input);
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
	lstate.collection->Append(lstate.append_state, chunk);
	gstate.loaded_count += chunk.size();
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

class MaterializedIndexConstructTask final : public ExecutorTask {
public:
	MaterializedIndexConstructTask(shared_ptr<Event> event_p, ClientContext &context,
	                               CreateMaterializedIndexGlobalSinkState &gstate_p, size_t thread_id_p,
	                               const PhysicalCreateIndexMaterialized &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), gstate(gstate_p), thread_id(thread_id_p), op_materialized(op_p),
	      local_scan_state() {

		// Initialize the scan chunk
		// TODO make this abstract?
		gstate.collection->InitializeScanChunk(scan_chunk);
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		return op_materialized.index_type.build_exec_task(mode);
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
		: BasePipelineEvent(input.pipeline), op(input.op), gstate(input.gstate),
		  info(input.info), storage_ids(input.storage_ids), table(input.table) {}

	const PhysicalCreateIndexMaterialized &op;
	CreateMaterializedIndexGlobalSinkState &gstate;
	CreateIndexInfo &info;
	const vector<column_t> &storage_ids;
	DuckTableEntry &table;

public:
	void Schedule() override {
		if (!op.index_type.build_schedule_event) {
			throw TransactionException("'build_schedule_event' callback is missing");
		}
		op.index_type.build_schedule_event();
	}

	void FinishEvent() override {
		if (!op.index_type.build_finish_event) {
			throw TransactionException("'build_finish_event' callback is missing");
		}
		op.index_type.build_finish_event();
	}
};

// TODO; Implement a callback for the sort
// And a callback for the exact count

SinkFinalizeType PhysicalCreateIndexMaterialized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                           OperatorSinkFinalizeInput &input) const {
	// Get the global collection we've been appending to
	auto &gstate = input.global_state.Cast<CreateMaterializedIndexGlobalSinkState>();

	index_build_finalize_t build_finalize = nullptr;
	// build finalize needs exact count as input
	index_build_finalize_count_t build_finalize_count = nullptr;
	// build finalize needs sorted data as input
	index_build_finalize_sort_t build_finalize_sort = nullptr;
	// build finalize needs counted + sorted
	index_build_finalize_count_sort_t build_finalize_count_and_sort = nullptr;

	// the collection contains the exact count
	auto count = gstate.collection->Count();
	// Finalize the index
	IndexBuildFinalizeInput finalize_input (*gstate.gstate);

	if (index_type.build_count) {
		finalize_input.has_count  = index_type.build_count;
	}

	bool is_sorted = false;
	if (index_type.build_sort) {
		is_sorted = index_type.build_count();
	}

	auto bound_index = index_type.build_finalize(finalize_input);

	// Move on to the next phase
	gstate.is_building = true;
	info->column_ids = storage_ids;

	// Reserve the index size
	auto &ts = TaskScheduler::GetScheduler(context);

	// this is a usearchindex
	auto &index = gstate.global_index->index;

	// here we use the count to reserve the index
	index.reserve({static_cast<size_t>(collection->Count()), static_cast<size_t>(ts.NumberOfThreads())});

	// Initialize a parallel scan for the index construction
	collection->InitializeScan(gstate.scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);

	// Create a new event that will construct the index
	auto index_construction_event_input = IndexConstructionEventInput(*this, gstate, pipeline, *info, storage_ids, table);
	auto new_event = make_shared_ptr<IndexConstructionEvent>(index_construction_event_input);
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
