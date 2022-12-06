#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"

#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

PhysicalBatchInsert::PhysicalBatchInsert(vector<LogicalType> types, TableCatalogEntry *table,
                                         physical_index_vector_t<idx_t> column_index_map,
                                         vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_INSERT, move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(table), insert_types(table->GetTypes()),
      bound_defaults(move(bound_defaults)) {
}

PhysicalBatchInsert::PhysicalBatchInsert(LogicalOperator &op, SchemaCatalogEntry *schema,
                                         unique_ptr<BoundCreateTableInfo> info_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_CREATE_TABLE_AS, op.types, estimated_cardinality),
      insert_table(nullptr), schema(schema), info(move(info_p)) {
	PhysicalInsert::GetInsertInfo(*info, insert_types, bound_defaults);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class CollectionMerger {
public:
	explicit CollectionMerger(ClientContext &context) : context(context) {
	}

	ClientContext &context;
	vector<unique_ptr<RowGroupCollection>> current_collections;

public:
	void AddCollection(unique_ptr<RowGroupCollection> collection) {
		current_collections.push_back(move(collection));
	}

	bool Empty() {
		return current_collections.empty();
	}

	unique_ptr<RowGroupCollection> Flush(OptimisticDataWriter &writer) {
		if (Empty()) {
			return nullptr;
		}
		unique_ptr<RowGroupCollection> new_collection = move(current_collections[0]);
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
						writer.CheckFlushToDisk(*new_collection);
					}
				}
			}

			new_collection->FinalizeAppend(TransactionData(0, 0), append_state);
			writer.FlushToDisk(*new_collection);
		}
		current_collections.clear();
		return new_collection;
	}
};

class BatchInsertGlobalState : public GlobalSinkState {
public:
	explicit BatchInsertGlobalState() : insert_count(0) {
	}

	mutex lock;
	TableCatalogEntry *table;
	idx_t insert_count;
	map<idx_t, unique_ptr<RowGroupCollection>> collections;

	bool CheckMergeInternal(idx_t batch_index, vector<unique_ptr<RowGroupCollection>> *result, idx_t *merge_count) {
		auto entry = collections.find(batch_index);
		if (entry == collections.end()) {
			// no collection at this index
			return false;
		}
		auto row_count = entry->second->GetTotalRows();
		if (row_count >= LocalStorage::MERGE_THRESHOLD) {
			// the collection at this batch index is large and has already been written
			return false;
		}
		// we can merge this collection!
		if (merge_count) {
			// add the count
			D_ASSERT(!result);
			*merge_count += row_count;
		} else {
			// add the
			D_ASSERT(result);
			result->push_back(move(entry->second));
			collections.erase(batch_index);
		}
		return true;
	}

	bool CheckMerge(idx_t batch_index, idx_t &merge_count) {
		return CheckMergeInternal(batch_index, nullptr, &merge_count);
	}
	bool CheckMerge(idx_t batch_index, vector<unique_ptr<RowGroupCollection>> &result) {
		return CheckMergeInternal(batch_index, &result, nullptr);
	}

	unique_ptr<RowGroupCollection> MergeCollections(ClientContext &context,
	                                                vector<unique_ptr<RowGroupCollection>> merge_collections,
	                                                OptimisticDataWriter &writer) {
		CollectionMerger merger(context);
		for (auto &collection : merge_collections) {
			merger.AddCollection(move(collection));
		}
		return merger.Flush(writer);
	}

	void VerifyUniqueBatch(idx_t batch_index) {
		if (collections.find(batch_index) != collections.end()) {
			throw InternalException("PhysicalBatchInsert::AddCollection error: batch index %d is present in multiple "
			                        "collections. This occurs when "
			                        "batch indexes are not uniquely distributed over threads",
			                        batch_index);
		}
	}

	void AddCollection(ClientContext &context, idx_t batch_index, unique_ptr<RowGroupCollection> current_collection,
	                   OptimisticDataWriter *writer = nullptr, bool *written_to_disk = nullptr) {
		vector<unique_ptr<RowGroupCollection>> merge_collections;
		idx_t merge_count;
		{
			lock_guard<mutex> l(lock);
			auto new_count = current_collection->GetTotalRows();
			insert_count += new_count;
			VerifyUniqueBatch(batch_index);
			if (writer && new_count < LocalStorage::MERGE_THRESHOLD) {
				// we are inserting a small collection that has not yet been written to disk
				// check if there are any collections with adjacent batch indexes that we can merge together

				// first check how many rows we will end up with by performing such a merge
				// check backwards
				merge_count = new_count;
				idx_t start_batch_index;
				idx_t end_batch_index;
				for (start_batch_index = batch_index; start_batch_index > 0; start_batch_index--) {
					if (!CheckMerge(start_batch_index - 1, merge_count)) {
						break;
					}
				}
				// check forwards
				for (end_batch_index = batch_index;; end_batch_index++) {
					if (!CheckMerge(end_batch_index + 1, merge_count)) {
						break;
					}
				}
				// merging together creates a big enough row group
				// merge!
				if (merge_count >= RowGroup::ROW_GROUP_SIZE) {
					// gather the row groups to merge
					// note that we need to gather them in order of batch index
					for (idx_t i = start_batch_index; i <= end_batch_index; i++) {
						if (i == batch_index) {
							merge_collections.push_back(move(current_collection));
							continue;
						}
						auto can_merge = CheckMerge(i, merge_collections);
						if (!can_merge) {
							throw InternalException("Could not merge row group in batch insert?!");
						}
					}
				}
			}
			if (merge_collections.empty()) {
				// no collections to merge together - add the collection to the batch index
				collections[batch_index] = move(current_collection);
			}
		}
		if (!merge_collections.empty()) {
			// merge together the collections
			D_ASSERT(writer);
			auto final_collection = MergeCollections(context, move(merge_collections), *writer);
			D_ASSERT(final_collection->GetTotalRows() == merge_count);
			D_ASSERT(final_collection->GetTotalRows() >= RowGroup::ROW_GROUP_SIZE);
			if (written_to_disk) {
				*written_to_disk = true;
			}
			// add the merged-together collection to the
			{
				lock_guard<mutex> l(lock);
				VerifyUniqueBatch(batch_index);
				collections[batch_index] = move(final_collection);
			}
		}
	}
};

class BatchInsertLocalState : public LocalSinkState {
public:
	BatchInsertLocalState(ClientContext &context, const vector<LogicalType> &types,
	                      const vector<unique_ptr<Expression>> &bound_defaults)
	    : default_executor(context, bound_defaults), written_to_disk(false) {
		insert_chunk.Initialize(Allocator::Get(context), types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
	idx_t current_index;
	TableAppendState current_append_state;
	unique_ptr<RowGroupCollection> current_collection;
	OptimisticDataWriter *writer;
	bool written_to_disk;

	void FlushToDisk() {
		if (!current_collection) {
			return;
		}
		if (!written_to_disk && current_collection->GetTotalRows() < LocalStorage::MERGE_THRESHOLD) {
			return;
		}
		writer->FlushToDisk(*current_collection, true);
	}

	void CreateNewCollection(TableCatalogEntry *table, const vector<LogicalType> &insert_types) {
		auto &table_info = table->storage->info;
		auto &block_manager = TableIOManager::Get(*table->storage).GetBlockManagerForRowData();
		current_collection = make_unique<RowGroupCollection>(table_info, block_manager, insert_types, MAX_ROW_ID);
		current_collection->InitializeEmpty();
		current_collection->InitializeAppend(current_append_state);
		written_to_disk = false;
	}
};

unique_ptr<GlobalSinkState> PhysicalBatchInsert::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_unique<BatchInsertGlobalState>();
	if (info) {
		// CREATE TABLE AS
		D_ASSERT(!insert_table);
		auto &catalog = *schema->catalog;
		result->table = (TableCatalogEntry *)catalog.CreateTable(context, schema, info.get());
	} else {
		D_ASSERT(insert_table);
		result->table = insert_table;
	}
	return move(result);
}

unique_ptr<LocalSinkState> PhysicalBatchInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BatchInsertLocalState>(context.client, insert_types, bound_defaults);
}

SinkResultType PhysicalBatchInsert::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                         DataChunk &chunk) const {
	auto &gstate = (BatchInsertGlobalState &)state;
	auto &lstate = (BatchInsertLocalState &)lstate_p;

	auto table = gstate.table;
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	if (!lstate.current_collection) {
		lock_guard<mutex> l(gstate.lock);
		// no collection yet: create a new one
		lstate.CreateNewCollection(table, insert_types);
		lstate.writer = gstate.table->storage->CreateOptimisticWriter(context.client);
	} else if (lstate.current_index != lstate.batch_index) {
		// batch index has changed: move the old collection to the global state and create a new collection
		TransactionData tdata(0, 0);
		lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
		lstate.FlushToDisk();
		gstate.AddCollection(context.client, lstate.current_index, move(lstate.current_collection), lstate.writer,
		                     &lstate.written_to_disk);
		lstate.CreateNewCollection(table, insert_types);
	}
	lstate.current_index = lstate.batch_index;
	table->storage->VerifyAppendConstraints(*table, context.client, lstate.insert_chunk);
	auto new_row_group = lstate.current_collection->Append(lstate.insert_chunk, lstate.current_append_state);
	if (new_row_group) {
		lstate.writer->CheckFlushToDisk(*lstate.current_collection);
		lstate.written_to_disk = true;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchInsert::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                  LocalSinkState &lstate_p) const {
	auto &gstate = (BatchInsertGlobalState &)gstate_p;
	auto &lstate = (BatchInsertLocalState &)lstate_p;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &lstate.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);

	if (!lstate.current_collection) {
		return;
	}
	lstate.FlushToDisk();
	lstate.writer->FinalFlush();

	TransactionData tdata(0, 0);
	lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
	gstate.AddCollection(context.client, lstate.current_index, move(lstate.current_collection));
}

SinkFinalizeType PhysicalBatchInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {
	auto &gstate = (BatchInsertGlobalState &)gstate_p;

	// in the finalize, do a final pass over all of the collections we created and try to merge smaller collections
	// together
	vector<unique_ptr<CollectionMerger>> mergers;
	unique_ptr<CollectionMerger> current_merger;

	auto &storage = *gstate.table->storage;
	for (auto &collection : gstate.collections) {
		if (collection.second->GetTotalRows() < LocalStorage::MERGE_THRESHOLD) {
			// this collection has very few rows: add it to the merge set
			if (!current_merger) {
				current_merger = make_unique<CollectionMerger>(context);
			}
			current_merger->AddCollection(move(collection.second));
		} else {
			// this collection has a lot of rows: it does not need to be merged
			// create a separate collection merger only for this entry
			if (current_merger) {
				// we have small collections remaining: flush them
				mergers.push_back(move(current_merger));
				current_merger.reset();
			}
			auto larger_merger = make_unique<CollectionMerger>(context);
			larger_merger->AddCollection(move(collection.second));
			mergers.push_back(move(larger_merger));
		}
	}
	if (current_merger) {
		mergers.push_back(move(current_merger));
	}

	// now that we have created all of the mergers, perform the actual merging
	vector<unique_ptr<RowGroupCollection>> final_collections;
	final_collections.reserve(mergers.size());
	auto writer = storage.CreateOptimisticWriter(context);
	for (auto &merger : mergers) {
		final_collections.push_back(merger->Flush(*writer));
	}
	writer->FinalFlush();

	// finally, merge the row groups into the local storage
	for (auto &collection : final_collections) {
		storage.LocalMerge(context, *collection);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class BatchInsertSourceState : public GlobalSourceState {
public:
	explicit BatchInsertSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalBatchInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<BatchInsertSourceState>();
}

void PhysicalBatchInsert::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	auto &state = (BatchInsertSourceState &)gstate;
	auto &insert_gstate = (BatchInsertGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
	state.finished = true;
	return;
}

} // namespace duckdb
