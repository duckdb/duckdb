#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/execution/operator/persistent/batch_memory_manager.hpp"
#include "duckdb/execution/operator/persistent/batch_task_manager.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

PhysicalBatchInsert::PhysicalBatchInsert(vector<LogicalType> types_p, TableCatalogEntry &table,
                                         physical_index_vector_t<idx_t> column_index_map_p,
                                         vector<unique_ptr<Expression>> bound_defaults_p,
                                         vector<unique_ptr<BoundConstraint>> bound_constraints_p,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_INSERT, std::move(types_p), estimated_cardinality),
      column_index_map(std::move(column_index_map_p)), insert_table(&table), insert_types(table.GetTypes()),
      bound_defaults(std::move(bound_defaults_p)), bound_constraints(std::move(bound_constraints_p)) {
}

PhysicalBatchInsert::PhysicalBatchInsert(LogicalOperator &op, SchemaCatalogEntry &schema,
                                         unique_ptr<BoundCreateTableInfo> info_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_CREATE_TABLE_AS, op.types, estimated_cardinality),
      insert_table(nullptr), schema(&schema), info(std::move(info_p)) {
	PhysicalInsert::GetInsertInfo(*info, insert_types, bound_defaults);
}

//===--------------------------------------------------------------------===//
// CollectionMerger
//===--------------------------------------------------------------------===//
class CollectionMerger {
public:
	explicit CollectionMerger(ClientContext &context) : context(context) {
	}

	ClientContext &context;
	vector<unique_ptr<RowGroupCollection>> current_collections;

public:
	void AddCollection(unique_ptr<RowGroupCollection> collection) {
		current_collections.push_back(std::move(collection));
	}

	bool Empty() {
		return current_collections.empty();
	}

	unique_ptr<RowGroupCollection> Flush(OptimisticDataWriter &writer) {
		if (Empty()) {
			return nullptr;
		}
		unique_ptr<RowGroupCollection> new_collection = std::move(current_collections[0]);
		if (current_collections.size() > 1) {
			// we have gathered multiple collections: create one big collection and merge that
			auto &types = new_collection->GetTypes();
			TableAppendState append_state;
			new_collection->InitializeAppend(append_state);

			DataChunk scan_chunk;
			scan_chunk.Initialize(context, types);

			vector<column_t> column_ids;
			for (idx_t i = 0; i < types.size(); i++) {
				column_ids.push_back(i);
			}
			for (auto &collection : current_collections) {
				if (!collection) {
					continue;
				}
				TableScanState scan_state;
				scan_state.Initialize(column_ids);
				collection->InitializeScan(scan_state.local_state, column_ids, nullptr);

				while (true) {
					scan_chunk.Reset();
					scan_state.local_state.ScanCommitted(scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
					if (scan_chunk.size() == 0) {
						break;
					}
					auto new_row_group = new_collection->Append(scan_chunk, append_state);
					if (new_row_group) {
						writer.WriteNewRowGroup(*new_collection);
					}
				}
			}
			new_collection->FinalizeAppend(TransactionData(0, 0), append_state);
			writer.WriteLastRowGroup(*new_collection);
		}
		current_collections.clear();
		return new_collection;
	}
};

enum class RowGroupBatchType : uint8_t { FLUSHED, NOT_FLUSHED };
struct RowGroupBatchEntry {
	RowGroupBatchEntry(idx_t batch_idx, unique_ptr<RowGroupCollection> collection_p, RowGroupBatchType type)
	    : batch_idx(batch_idx), total_rows(collection_p->GetTotalRows()), unflushed_memory(0),
	      collection(std::move(collection_p)), type(type) {
		if (type == RowGroupBatchType::NOT_FLUSHED) {
			unflushed_memory = collection->GetAllocationSize();
		}
	}

	idx_t batch_idx;
	idx_t total_rows;
	idx_t unflushed_memory;
	unique_ptr<RowGroupCollection> collection;
	RowGroupBatchType type;
};

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class BatchInsertTask {
public:
	virtual ~BatchInsertTask() {
	}

	virtual void Execute(const PhysicalBatchInsert &op, ClientContext &context, GlobalSinkState &gstate_p,
	                     LocalSinkState &lstate_p) = 0;
};

class BatchInsertGlobalState : public GlobalSinkState {
public:
	explicit BatchInsertGlobalState(ClientContext &context, DuckTableEntry &table, idx_t minimum_memory_per_thread)
	    : memory_manager(context, minimum_memory_per_thread), table(table), insert_count(0),
	      optimistically_written(false), minimum_memory_per_thread(minimum_memory_per_thread) {
	}

	BatchMemoryManager memory_manager;
	BatchTaskManager<BatchInsertTask> task_manager;
	mutex lock;
	DuckTableEntry &table;
	idx_t insert_count;
	vector<RowGroupBatchEntry> collections;
	idx_t next_start = 0;
	atomic<bool> optimistically_written;
	idx_t minimum_memory_per_thread;

	static bool ReadyToMerge(idx_t count);
	void ScheduleMergeTasks(idx_t min_batch_index);
	unique_ptr<RowGroupCollection> MergeCollections(ClientContext &context,
	                                                vector<RowGroupBatchEntry> merge_collections,
	                                                OptimisticDataWriter &writer);
	void AddCollection(ClientContext &context, idx_t batch_index, idx_t min_batch_index,
	                   unique_ptr<RowGroupCollection> current_collection,
	                   optional_ptr<OptimisticDataWriter> writer = nullptr);

	idx_t MaxThreads(idx_t source_max_threads) override {
		// try to request 4MB per column per thread
		memory_manager.SetMemorySize(source_max_threads * minimum_memory_per_thread);
		// cap the concurrent threads working on this task based on the amount of available memory
		return MinValue<idx_t>(source_max_threads, memory_manager.AvailableMemory() / minimum_memory_per_thread + 1);
	}
};

class BatchInsertLocalState : public LocalSinkState {
public:
	BatchInsertLocalState(ClientContext &context, const vector<LogicalType> &types,
	                      const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(context, bound_defaults) {
		insert_chunk.Initialize(Allocator::Get(context), types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
	idx_t current_index;
	TableAppendState current_append_state;
	unique_ptr<RowGroupCollection> current_collection;
	optional_ptr<OptimisticDataWriter> writer;
	unique_ptr<ConstraintState> constraint_state;

	void CreateNewCollection(DuckTableEntry &table, const vector<LogicalType> &insert_types) {
		auto table_info = table.GetStorage().GetDataTableInfo();
		auto &block_manager = TableIOManager::Get(table.GetStorage()).GetBlockManagerForRowData();
		current_collection = make_uniq<RowGroupCollection>(std::move(table_info), block_manager, insert_types,
		                                                   NumericCast<idx_t>(MAX_ROW_ID));
		current_collection->InitializeEmpty();
		current_collection->InitializeAppend(current_append_state);
	}
};

//===--------------------------------------------------------------------===//
// Merge Task
//===--------------------------------------------------------------------===//
class MergeCollectionTask : public BatchInsertTask {
public:
	MergeCollectionTask(vector<RowGroupBatchEntry> merge_collections_p, idx_t merged_batch_index)
	    : merge_collections(std::move(merge_collections_p)), merged_batch_index(merged_batch_index) {
	}

	vector<RowGroupBatchEntry> merge_collections;
	idx_t merged_batch_index;

	void Execute(const PhysicalBatchInsert &op, ClientContext &context, GlobalSinkState &gstate_p,
	             LocalSinkState &lstate_p) override {
		auto &gstate = gstate_p.Cast<BatchInsertGlobalState>();
		auto &lstate = lstate_p.Cast<BatchInsertLocalState>();
		// merge together the collections
		D_ASSERT(lstate.writer);
		auto final_collection = gstate.MergeCollections(context, std::move(merge_collections), *lstate.writer);
		// add the merged-together collection to the set of batch indexes
		lock_guard<mutex> l(gstate.lock);
		RowGroupBatchEntry new_entry(merged_batch_index, std::move(final_collection), RowGroupBatchType::FLUSHED);
		auto it = std::lower_bound(
		    gstate.collections.begin(), gstate.collections.end(), new_entry,
		    [&](const RowGroupBatchEntry &a, const RowGroupBatchEntry &b) { return a.batch_idx < b.batch_idx; });
		if (it->batch_idx != merged_batch_index) {
			throw InternalException("Merged batch index was no longer present in collection");
		}
		it->collection = std::move(new_entry.collection);
	}
};

struct BatchMergeTask {
	explicit BatchMergeTask(idx_t start_index) : start_index(start_index), end_index(0), total_count(0) {
	}

	idx_t start_index;
	idx_t end_index;
	idx_t total_count;
};

bool BatchInsertGlobalState::ReadyToMerge(idx_t count) {
	// we try to merge so the count fits nicely into row groups
	if (count >= Storage::ROW_GROUP_SIZE / 10 * 9 && count <= Storage::ROW_GROUP_SIZE) {
		// 90%-100% of row group size
		return true;
	}
	if (count >= Storage::ROW_GROUP_SIZE / 10 * 18 && count <= Storage::ROW_GROUP_SIZE * 2) {
		// 180%-200% of row group size
		return true;
	}
	if (count >= Storage::ROW_GROUP_SIZE / 10 * 27 && count <= Storage::ROW_GROUP_SIZE * 3) {
		// 270%-300% of row group size
		return true;
	}
	if (count >= Storage::ROW_GROUP_SIZE / 10 * 36) {
		// >360% of row group size
		return true;
	}
	return false;
}

void BatchInsertGlobalState::ScheduleMergeTasks(idx_t min_batch_index) {
	idx_t current_idx;

	vector<BatchMergeTask> to_be_scheduled_tasks;

	BatchMergeTask current_task(next_start);
	for (current_idx = current_task.start_index; current_idx < collections.size(); current_idx++) {
		auto &entry = collections[current_idx];
		if (entry.batch_idx > min_batch_index) {
			// this entry is AFTER the min_batch_index
			// finished
			if (ReadyToMerge(current_task.total_count)) {
				current_task.end_index = current_idx;
				to_be_scheduled_tasks.push_back(current_task);
			}
			break;
		}
		if (entry.type == RowGroupBatchType::FLUSHED) {
			// already flushed: cannot flush anything here
			if (current_task.total_count > 0) {
				current_task.end_index = current_idx;
				to_be_scheduled_tasks.push_back(current_task);
			}
			current_task.start_index = current_idx + 1;
			if (current_task.start_index > next_start) {
				// avoid checking this segment again in the future
				next_start = current_task.start_index;
			}
			current_task.total_count = 0;
			continue;
		}
		// not flushed - add to set of indexes to flush
		current_task.total_count += entry.total_rows;
		if (ReadyToMerge(current_task.total_count)) {
			// create a task to merge these collections
			current_task.end_index = current_idx + 1;
			to_be_scheduled_tasks.push_back(current_task);
			current_task.start_index = current_idx + 1;
			current_task.total_count = 0;
		}
	}

	if (to_be_scheduled_tasks.empty()) {
		return;
	}
	for (auto &scheduled_task : to_be_scheduled_tasks) {
		D_ASSERT(scheduled_task.total_count > 0);
		D_ASSERT(current_idx > scheduled_task.start_index);
		idx_t merged_batch_index = collections[scheduled_task.start_index].batch_idx;
		vector<RowGroupBatchEntry> merge_collections;
		for (idx_t idx = scheduled_task.start_index; idx < scheduled_task.end_index; idx++) {
			auto &entry = collections[idx];
			if (!entry.collection || entry.type == RowGroupBatchType::FLUSHED) {
				throw InternalException("Adding a row group collection that should not be flushed");
			}
			RowGroupBatchEntry added_entry(collections[scheduled_task.start_index].batch_idx,
			                               std::move(entry.collection), RowGroupBatchType::FLUSHED);
			added_entry.unflushed_memory = entry.unflushed_memory;
			merge_collections.push_back(std::move(added_entry));
			entry.total_rows = scheduled_task.total_count;
			entry.type = RowGroupBatchType::FLUSHED;
		}
		task_manager.AddTask(make_uniq<MergeCollectionTask>(std::move(merge_collections), merged_batch_index));
	}
	// erase in reverse order
	for (idx_t i = to_be_scheduled_tasks.size(); i > 0; i--) {
		auto &scheduled_task = to_be_scheduled_tasks[i - 1];
		if (scheduled_task.start_index + 1 < scheduled_task.end_index) {
			// erase all entries except the first one
			collections.erase(collections.begin() + NumericCast<int64_t>(scheduled_task.start_index) + 1,
			                  collections.begin() + NumericCast<int64_t>(scheduled_task.end_index));
		}
	}
}

unique_ptr<RowGroupCollection> BatchInsertGlobalState::MergeCollections(ClientContext &context,
                                                                        vector<RowGroupBatchEntry> merge_collections,
                                                                        OptimisticDataWriter &writer) {
	D_ASSERT(!merge_collections.empty());
	CollectionMerger merger(context);
	idx_t written_data = 0;
	for (auto &entry : merge_collections) {
		merger.AddCollection(std::move(entry.collection));
		written_data += entry.unflushed_memory;
	}
	optimistically_written = true;
	memory_manager.ReduceUnflushedMemory(written_data);
	return merger.Flush(writer);
}

void BatchInsertGlobalState::AddCollection(ClientContext &context, idx_t batch_index, idx_t min_batch_index,
                                           unique_ptr<RowGroupCollection> current_collection,
                                           optional_ptr<OptimisticDataWriter> writer) {
	if (batch_index < min_batch_index) {
		throw InternalException("Batch index of the added collection (%llu) is smaller than the min batch index (%llu)",
		                        batch_index, min_batch_index);
	}
	auto new_count = current_collection->GetTotalRows();
	auto batch_type = new_count < Storage::ROW_GROUP_SIZE ? RowGroupBatchType::NOT_FLUSHED : RowGroupBatchType::FLUSHED;
	if (batch_type == RowGroupBatchType::FLUSHED && writer) {
		writer->WriteLastRowGroup(*current_collection);
	}
	lock_guard<mutex> l(lock);
	insert_count += new_count;
	// add the collection to the batch index
	RowGroupBatchEntry new_entry(batch_index, std::move(current_collection), batch_type);
	if (batch_type == RowGroupBatchType::NOT_FLUSHED) {
		memory_manager.IncreaseUnflushedMemory(new_entry.unflushed_memory);
	}

	auto it = std::lower_bound(
	    collections.begin(), collections.end(), new_entry,
	    [&](const RowGroupBatchEntry &a, const RowGroupBatchEntry &b) { return a.batch_idx < b.batch_idx; });
	if (it != collections.end() && it->batch_idx == new_entry.batch_idx) {
		throw InternalException("PhysicalBatchInsert::AddCollection error: batch index %d is present in multiple "
		                        "collections. This occurs when "
		                        "batch indexes are not uniquely distributed over threads",
		                        batch_index);
	}
	collections.insert(it, std::move(new_entry));
	if (writer) {
		ScheduleMergeTasks(min_batch_index);
	}
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalBatchInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<TableCatalogEntry> table;
	if (info) {
		// CREATE TABLE AS
		D_ASSERT(!insert_table);
		auto &catalog = schema->catalog;
		auto created_table = catalog.CreateTable(catalog.GetCatalogTransaction(context), *schema.get_mutable(), *info);
		table = &created_table->Cast<TableCatalogEntry>();
	} else {
		D_ASSERT(insert_table);
		D_ASSERT(insert_table->IsDuckTable());
		table = insert_table.get_mutable();
	}
	// heuristic - we start off by allocating 4MB of cache space per column
	static constexpr const idx_t MINIMUM_MEMORY_PER_COLUMN = 4ULL * 1024ULL * 1024ULL;
	auto minimum_memory_per_thread = table->GetColumns().PhysicalColumnCount() * MINIMUM_MEMORY_PER_COLUMN;
	auto result = make_uniq<BatchInsertGlobalState>(context, table->Cast<DuckTableEntry>(), minimum_memory_per_thread);
	return std::move(result);
}

unique_ptr<LocalSinkState> PhysicalBatchInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BatchInsertLocalState>(context.client, insert_types, bound_defaults);
}

//===--------------------------------------------------------------------===//
// Tasks
//===--------------------------------------------------------------------===//
bool PhysicalBatchInsert::ExecuteTask(ClientContext &context, GlobalSinkState &gstate_p,
                                      LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<BatchInsertGlobalState>();
	auto task = gstate.task_manager.GetTask();
	if (!task) {
		return false;
	}
	task->Execute(*this, context, gstate_p, lstate_p);
	return true;
}

void PhysicalBatchInsert::ExecuteTasks(ClientContext &context, GlobalSinkState &gstate_p,
                                       LocalSinkState &lstate_p) const {
	while (ExecuteTask(context, gstate_p, lstate_p)) {
	}
}

//===--------------------------------------------------------------------===//
// NextBatch
//===--------------------------------------------------------------------===//
SinkNextBatchType PhysicalBatchInsert::NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const {
	auto &gstate = input.global_state.Cast<BatchInsertGlobalState>();
	auto &lstate = input.local_state.Cast<BatchInsertLocalState>();
	auto &memory_manager = gstate.memory_manager;

	auto batch_index = lstate.partition_info.batch_index.GetIndex();
	if (lstate.current_collection) {
		if (lstate.current_index == batch_index) {
			throw InternalException("NextBatch called with the same batch index?");
		}
		// batch index has changed: move the old collection to the global state and create a new collection
		TransactionData tdata(0, 0);
		lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
		gstate.AddCollection(context.client, lstate.current_index, lstate.partition_info.min_batch_index.GetIndex(),
		                     std::move(lstate.current_collection), lstate.writer);

		auto any_unblocked = memory_manager.UnblockTasks();
		if (!any_unblocked) {
			ExecuteTasks(context.client, gstate, lstate);
		}
		lstate.current_collection.reset();
	}
	lstate.current_index = batch_index;

	// unblock any blocked tasks
	memory_manager.UnblockTasks();

	return SinkNextBatchType::READY;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalBatchInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BatchInsertGlobalState>();
	auto &lstate = input.local_state.Cast<BatchInsertLocalState>();
	auto &memory_manager = gstate.memory_manager;

	auto &table = gstate.table;
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	auto batch_index = lstate.partition_info.batch_index.GetIndex();
	// check if we should process this batch
	if (!memory_manager.IsMinimumBatchIndex(batch_index)) {
		memory_manager.UpdateMinBatchIndex(lstate.partition_info.min_batch_index.GetIndex());

		// we are not processing the current min batch index
		// check if we have exceeded the maximum number of unflushed rows
		if (memory_manager.OutOfMemory(batch_index)) {
			// out-of-memory
			// execute tasks while we wait (if any are available)
			ExecuteTasks(context.client, gstate, lstate);

			lock_guard<mutex> l(memory_manager.GetBlockedTaskLock());
			if (!memory_manager.IsMinimumBatchIndex(batch_index)) {
				//  we are not the minimum batch index and we have no memory available to buffer - block the task for
				//  now
				memory_manager.BlockTask(input.interrupt_state);
				return SinkResultType::BLOCKED;
			}
		}
	}
	if (!lstate.current_collection) {
		lock_guard<mutex> l(gstate.lock);
		// no collection yet: create a new one
		lstate.CreateNewCollection(table, insert_types);
		if (!lstate.writer) {
			lstate.writer = &table.GetStorage().CreateOptimisticWriter(context.client);
		}
	}

	if (lstate.current_index != batch_index) {
		throw InternalException("Current batch differs from batch - but NextBatch was not called!?");
	}

	if (!lstate.constraint_state) {
		lstate.constraint_state = table.GetStorage().InitializeConstraintState(table, bound_constraints);
	}
	table.GetStorage().VerifyAppendConstraints(*lstate.constraint_state, context.client, lstate.insert_chunk);

	auto new_row_group = lstate.current_collection->Append(lstate.insert_chunk, lstate.current_append_state);
	if (new_row_group) {
		// we have already written to disk - flush the next row group as well
		lstate.writer->WriteNewRowGroup(*lstate.current_collection);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalBatchInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<BatchInsertGlobalState>();
	auto &lstate = input.local_state.Cast<BatchInsertLocalState>();
	auto &memory_manager = gstate.memory_manager;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this, lstate.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);

	memory_manager.UpdateMinBatchIndex(lstate.partition_info.min_batch_index.GetIndex());

	if (lstate.current_collection) {
		TransactionData tdata(0, 0);
		lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
		if (lstate.current_collection->GetTotalRows() > 0) {
			gstate.AddCollection(context.client, lstate.current_index, lstate.partition_info.min_batch_index.GetIndex(),
			                     std::move(lstate.current_collection));
		}
	}
	if (lstate.writer) {
		lock_guard<mutex> l(gstate.lock);
		gstate.table.GetStorage().FinalizeOptimisticWriter(context.client, *lstate.writer);
	}

	// unblock any blocked tasks
	memory_manager.UnblockTasks();

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalBatchInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BatchInsertGlobalState>();
	auto &memory_manager = gstate.memory_manager;

	if (gstate.optimistically_written || gstate.insert_count >= LocalStorage::MERGE_THRESHOLD) {
		// we have written data to disk optimistically or are inserting a large amount of data
		// perform a final pass over all of the row groups and merge them together
		vector<unique_ptr<CollectionMerger>> mergers;
		unique_ptr<CollectionMerger> current_merger;

		auto &storage = gstate.table.GetStorage();
		for (auto &entry : gstate.collections) {
			if (entry.type == RowGroupBatchType::NOT_FLUSHED) {
				// this collection has not been flushed: add it to the merge set
				if (!current_merger) {
					current_merger = make_uniq<CollectionMerger>(context);
				}
				current_merger->AddCollection(std::move(entry.collection));
				memory_manager.ReduceUnflushedMemory(entry.unflushed_memory);
			} else {
				// this collection has been flushed: it does not need to be merged
				// create a separate collection merger only for this entry
				if (current_merger) {
					// we have small collections remaining: flush them
					mergers.push_back(std::move(current_merger));
					current_merger.reset();
				}
				auto larger_merger = make_uniq<CollectionMerger>(context);
				larger_merger->AddCollection(std::move(entry.collection));
				mergers.push_back(std::move(larger_merger));
			}
		}
		if (current_merger) {
			mergers.push_back(std::move(current_merger));
		}

		// now that we have created all of the mergers, perform the actual merging
		vector<unique_ptr<RowGroupCollection>> final_collections;
		final_collections.reserve(mergers.size());
		auto &writer = storage.CreateOptimisticWriter(context);
		for (auto &merger : mergers) {
			final_collections.push_back(merger->Flush(writer));
		}

		// finally, merge the row groups into the local storage
		for (auto &collection : final_collections) {
			storage.LocalMerge(context, *collection);
		}
		storage.FinalizeOptimisticWriter(context, writer);
	} else {
		// we are writing a small amount of data to disk
		// append directly to transaction local storage
		auto &table = gstate.table;
		auto &storage = table.GetStorage();
		LocalAppendState append_state;
		storage.InitializeLocalAppend(append_state, table, context, bound_constraints);
		auto &transaction = DuckTransaction::Get(context, table.catalog);
		for (auto &entry : gstate.collections) {
			if (entry.type != RowGroupBatchType::NOT_FLUSHED) {
				throw InternalException("Encountered a flushed batch");
			}

			memory_manager.ReduceUnflushedMemory(entry.unflushed_memory);
			entry.collection->Scan(transaction, [&](DataChunk &insert_chunk) {
				storage.LocalAppend(append_state, table, context, insert_chunk);
				return true;
			});
		}
		storage.FinalizeLocalAppend(append_state);
	}
	memory_manager.FinalCheck();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalBatchInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<BatchInsertGlobalState>();

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));

	return SourceResultType::FINISHED;
}

} // namespace duckdb
