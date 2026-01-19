#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

PhysicalCreateIndex::PhysicalCreateIndex(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table_p,
                                         const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                         vector<unique_ptr<Expression>> unbound_expressions_p,
                                         idx_t estimated_cardinality, IndexType index_type,
                                         unique_ptr<IndexBuildBindData> bind_data,
                                         unique_ptr<AlterTableInfo> alter_table_info)

    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)),
      unbound_expressions(std::move(unbound_expressions_p)), index_type(std::move(index_type)),
      bind_data(std::move(bind_data)), alter_table_info(std::move(alter_table_info)) {
	// Convert the logical column ids to physical column ids.
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}

	for (idx_t i = 0; i < unbound_expressions.size(); ++i) {
		auto &expr = unbound_expressions[i];
		indexed_column_types.push_back(expr->return_type);
		indexed_columns.push_back(i);
	}

	// Row id is alway last
	rowid_column.push_back(unbound_expressions.size());
}

//---------------------------------------------------------------------------------------------------------------------
// Global Sink
//---------------------------------------------------------------------------------------------------------------------

unique_ptr<GlobalSinkState> PhysicalCreateIndex::GetGlobalSinkState(ClientContext &context) const {
	auto g_sink_state = make_uniq<CreateIndexGlobalSinkState>();
	IndexBuildInitStateInput global_state_input {
	    bind_data, context, table, *info, unbound_expressions, storage_ids, estimated_cardinality};

	g_sink_state->gstate = index_type.build_init(global_state_input);
	return std::move(g_sink_state);
}

//-------------------------------------------------------------
// Local Sink
//-------------------------------------------------------------

unique_ptr<LocalSinkState> PhysicalCreateIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto lstate = make_uniq<CreateIndexLocalSinkState>();

	vector<LogicalType> data_types;
	for (auto &expr : unbound_expressions) {
		data_types.push_back(expr->return_type);
	}

	IndexBuildInitSinkInput local_state_input {bind_data,  context.client,      table,      *info,
	                                           data_types, unbound_expressions, storage_ids};

	lstate->lstate = index_type.build_sink_init(local_state_input);
	lstate->key_chunk.InitializeEmpty(indexed_column_types);
	lstate->row_chunk.InitializeEmpty({LogicalType::ROW_TYPE});

	return std::move(lstate);
}

//-------------------------------------------------------------
// Sink
//-------------------------------------------------------------

// build_sink
SinkResultType PhysicalCreateIndex::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CreateIndexGlobalSinkState>();
	auto &lstate = input.local_state.Cast<CreateIndexLocalSinkState>();

	// FIXME: use unified format instead of Flatten
	chunk.Flatten();

	// Reference the key columns and rowid column
	lstate.key_chunk.ReferenceColumns(chunk, indexed_columns);
	lstate.row_chunk.ReferenceColumns(chunk, rowid_column);

	// Check for NULLs, if we are creating a PRIMARY KEY.
	// FIXME: Later, we want to ensure that we skip the NULL check for any non-PK alter.
	if (alter_table_info) {
		auto row_count = lstate.key_chunk.size();
		for (idx_t i = 0; i < lstate.key_chunk.ColumnCount(); i++) {
			if (VectorOperations::HasNull(lstate.key_chunk.data[i], row_count)) {
				throw ConstraintException("NOT NULL constraint failed: %s", info->index_name);
			}
		}
	}

	IndexBuildSinkInput sink_input {bind_data, gstate.gstate, lstate.lstate, table, *info};
	index_type.build_sink(sink_input, lstate.key_chunk, lstate.row_chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

// build_sink_combine
SinkCombineResultType PhysicalCreateIndex::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateIndexGlobalSinkState>();
	auto &lstate = input.local_state.Cast<CreateIndexLocalSinkState>();

	if (index_type.build_sink_combine) {
		IndexBuildSinkCombineInput combine_input {bind_data, gstate.gstate, lstate.lstate, table, *info};
		index_type.build_sink_combine(combine_input);
	}

	return SinkCombineResultType::FINISHED;
}

//-------------------------------------------------------------
// Work Phase
//-------------------------------------------------------------

class IndexConstructTask final : public ExecutorTask {
public:
	IndexConstructTask(shared_ptr<Event> event_p, ClientContext &context, CreateIndexGlobalSinkState &gstate_p,
	                   size_t thread_id_p, const PhysicalCreateIndex &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), gstate(gstate_p), thread_id(thread_id_p), index_op(op_p),
	      local_scan_state() {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		auto &work_state = gstate.work_states[thread_id];

		// TODO // what to do with this?
		// I think we should abstract this, otherwise we might complicate it for the user
		// if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
		// 	return TaskExecutionResult::TASK_NOT_FINISHED;
		// }

		// execute build_work (do work per task)
		IndexBuildWorkInput build_work_input {gstate.gstate, work_state, thread_id};

		bool keep_going = true;
		while (keep_going) {
			try {
				// Are we done?
				keep_going = index_op.index_type.build_work(build_work_input);
			} catch (std::exception &ex) {
				// Catch any exceptions and return errors!
				ErrorData err(ex);
				executor.PushError(err);
				return TaskExecutionResult::TASK_ERROR;
			}

			if (mode == TaskExecutionMode::PROCESS_PARTIAL && keep_going) {
				// We are running on the main thread and need to yield after each work iteration or risk blocking
				// other parts of the system!
				return TaskExecutionResult::TASK_NOT_FINISHED;
			}

			// Otherwise, we keep going until we are done
		}

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	CreateIndexGlobalSinkState &gstate;
	size_t thread_id;

	DataChunk scan_chunk;
	ColumnDataLocalScanState local_scan_state;
	const PhysicalCreateIndex &index_op;
};

struct IndexConstructionEventInput {
	IndexConstructionEventInput(const PhysicalCreateIndex &op, CreateIndexGlobalSinkState &gstate, Pipeline &pipeline,
	                            CreateIndexInfo &info, const vector<column_t> &storage_ids, DuckTableEntry &table)
	    : op(op), gstate(gstate), pipeline(pipeline), info(info), storage_ids(storage_ids), table(table) {
	}

	const PhysicalCreateIndex &op;
	CreateIndexGlobalSinkState &gstate;
	Pipeline &pipeline;
	CreateIndexInfo &info;
	const vector<column_t> &storage_ids;
	DuckTableEntry &table;
};

class IndexConstructionEvent final : public BasePipelineEvent {
public:
	explicit IndexConstructionEvent(const IndexConstructionEventInput &input)
	    : BasePipelineEvent(input.pipeline), op(input.op), gstate(input.gstate), info(input.info),
	      storage_ids(input.storage_ids), table(input.table), context(pipeline->GetClientContext()) {
	}

	const PhysicalCreateIndex &op;
	CreateIndexGlobalSinkState &gstate;
	CreateIndexInfo &info;
	const vector<column_t> &storage_ids;
	DuckTableEntry &table;
	ClientContext &context;

public:
	void Schedule() override {
		//! Midpoint
		if (op.index_type.build_prepare) {
			// prepare
			// TODO: Put this in a separate event-chain to not block in Schedule()
			IndexBuildPrepareInput prepare_input {context, gstate.gstate, op.bind_data};
			op.index_type.build_prepare(prepare_input);
		}

		vector<shared_ptr<Task>> tasks;
		idx_t num_tasks = 1;

		// We only schedule a single task if there is no work_init callback
		if (op.index_type.build_work_init) {
			// Schedule tasks equal to the number of threads
			auto &ts = TaskScheduler::GetScheduler(context);
			num_tasks = NumericCast<size_t>(ts.NumberOfThreads());
		}

		for (size_t tnum = 0; tnum < num_tasks; tnum++) {
			auto task = make_uniq<IndexConstructTask>(shared_from_this(), context, gstate, tnum, op);
			tasks.push_back(std::move(task));

			if (op.index_type.build_work_init) {
				// execute build_work_init and get a fresh worker state
				IndexBuildInitWorkInput build_work_input {op.bind_data, gstate.gstate};
				auto state = op.index_type.build_work_init(build_work_input);
				gstate.work_states.push_back(std::move(state));
			} else {
				// Add dummy state
				gstate.work_states.push_back(nullptr);
			}
		}

		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		// global and local states
		// TODO; itterate over this; this is not correct yet

		if (op.index_type.build_work_combine) {
			auto &g_workstate = gstate.gstate;
			// Combine each local work state into the global work state
			for (auto &l_workstate : gstate.work_states) {
				IndexBuildWorkCombineInput work_combine_input {op.bind_data, g_workstate, l_workstate};
				op.index_type.build_work_combine(work_combine_input);
			}
		}

		// Finally, call Finalize() to add the index to the storage
		op.FinalizeIndexBuild(context, gstate);
	}
};

//-------------------------------------------------------------
// Finalize
//-------------------------------------------------------------

void PhysicalCreateIndex::FinalizeIndexBuild(ClientContext &context, CreateIndexGlobalSinkState &state) const {
	// Vacuum excess memory and verify.

	IndexBuildFinalizeInput input(*state.gstate);
	auto final_index = this->index_type.build_finalize(input);

	final_index->Vacuum();

	final_index->Verify();

	D_ASSERT(!final_index->ToString(true).empty());

	final_index->VerifyAllocations();

	auto &storage = table.GetStorage();
	if (!storage.IsMainTable()) {
		throw TransactionException(
		    "Transaction conflict: cannot add an index to a table that has been altered or dropped");
	}

	auto &schema = table.schema;
	info->column_ids = storage_ids;

	if (!alter_table_info) {
		// Ensure that the index does not yet exist in the catalog.
		auto entry = schema.GetEntry(schema.GetCatalogTransaction(context), CatalogType::INDEX_ENTRY, info->index_name);
		if (entry) {
			if (info->on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
				throw CatalogException("Index with name \"%s\" already exists!", info->index_name);
			}
			// IF NOT EXISTS on existing index. We are done.
			return;
		}

		auto index_entry = schema.CreateIndex(schema.GetCatalogTransaction(context), *info, table).get();
		D_ASSERT(index_entry);
		auto &index = index_entry->Cast<DuckIndexEntry>();
		index.initial_index_size = final_index->GetInMemorySize();

	} else {
		// Ensure that there are no other indexes with that name on this table.
		auto &indexes = storage.GetDataTableInfo()->GetIndexes();
		indexes.Scan([&](Index &index) {
			if (index.GetIndexName() == info->index_name) {
				throw CatalogException("an index with that name already exists for this table: %s", info->index_name);
			}
			return false;
		});

		auto &catalog = Catalog::GetCatalog(context, info->catalog);
		catalog.Alter(context, *alter_table_info);
	}

	// Add the index to the storage.
	storage.AddIndex(std::move(final_index));
}

SinkFinalizeType PhysicalCreateIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CreateIndexGlobalSinkState>();

	// determine if we still need to do some processing before finalizing
	if (index_type.build_work && index_type.build_work_init) {
		// determine how many tasks we need to spawn
		auto index_construction_event_input =
		    IndexConstructionEventInput(*this, gstate, pipeline, *info, storage_ids, table);
		auto new_event = make_shared_ptr<IndexConstructionEvent>(index_construction_event_input);
		event.InsertEvent(std::move(new_event));
	} else {
		// Finalize the index
		FinalizeIndexBuild(context, gstate);
	}

	return SinkFinalizeType::READY;
}

} // namespace duckdb
