#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"

#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

PhysicalBatchInsert::PhysicalBatchInsert(vector<LogicalType> types, TableCatalogEntry &table,
                                         physical_index_vector_t<idx_t> column_index_map,
                                         vector<unique_ptr<Expression>> bound_defaults, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_INSERT, std::move(types), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(&table), insert_types(table.GetTypes()),
      bound_defaults(std::move(bound_defaults)) {
}

PhysicalBatchInsert::PhysicalBatchInsert(LogicalOperator &op, SchemaCatalogEntry &schema,
                                         unique_ptr<BoundCreateTableInfo> info_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_CREATE_TABLE_AS, op.types, estimated_cardinality),
      insert_table(nullptr), schema(&schema), info(std::move(info_p)) {
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
	    : batch_idx(batch_idx), total_rows(collection_p->GetTotalRows()), collection(std::move(collection_p)),
	      type(type) {
	}

	idx_t batch_idx;
	idx_t total_rows;
	unique_ptr<RowGroupCollection> collection;
	RowGroupBatchType type;
};

class BatchInsertGlobalState : public GlobalSinkState {
public:
	static constexpr const idx_t BATCH_FLUSH_THRESHOLD = LocalStorage::MERGE_THRESHOLD * 3;

public:
	explicit BatchInsertGlobalState(DuckTableEntry &table) : table(table), insert_count(0) {
	}

	mutex lock;
	DuckTableEntry &table;
	idx_t insert_count;
	vector<RowGroupBatchEntry> collections;
	idx_t next_start = 0;

	void FindMergeCollections(idx_t min_batch_index, optional_idx &merged_batch_index,
	                          vector<unique_ptr<RowGroupCollection>> &result) {
		bool merge = false;
		idx_t start_index = next_start;
		idx_t current_idx;
		idx_t total_count = 0;
		for (current_idx = start_index; current_idx < collections.size(); current_idx++) {
			auto &entry = collections[current_idx];
			if (entry.batch_idx >= min_batch_index) {
				// this entry is AFTER the min_batch_index
				// we might still find new entries!
				break;
			}
			if (entry.type == RowGroupBatchType::FLUSHED) {
				// already flushed: cannot flush anything here
				if (total_count > 0) {
					merge = true;
					break;
				}
				start_index = current_idx + 1;
				if (start_index > next_start) {
					// avoid checking this segment again in the future
					next_start = start_index;
				}
				total_count = 0;
				continue;
			}
			// not flushed - add to set of indexes to flush
			total_count += entry.total_rows;
			if (total_count >= BATCH_FLUSH_THRESHOLD) {
				merge = true;
				break;
			}
		}
		if (merge && total_count > 0) {
			D_ASSERT(current_idx > start_index);
			merged_batch_index = collections[start_index].batch_idx;
			for (idx_t idx = start_index; idx < current_idx; idx++) {
				auto &entry = collections[idx];
				if (!entry.collection || entry.type == RowGroupBatchType::FLUSHED) {
					throw InternalException("Adding a row group collection that should not be flushed");
				}
				result.push_back(std::move(entry.collection));
				entry.total_rows = total_count;
				entry.type = RowGroupBatchType::FLUSHED;
			}
			if (start_index + 1 < current_idx) {
				// erase all entries except the first one
				collections.erase(collections.begin() + start_index + 1, collections.begin() + current_idx);
			}
		}
	}

	unique_ptr<RowGroupCollection> MergeCollections(ClientContext &context,
	                                                vector<unique_ptr<RowGroupCollection>> merge_collections,
	                                                OptimisticDataWriter &writer) {
		CollectionMerger merger(context);
		for (auto &collection : merge_collections) {
			merger.AddCollection(std::move(collection));
		}
		return merger.Flush(writer);
	}

	void AddCollection(ClientContext &context, idx_t batch_index, idx_t min_batch_index,
	                   unique_ptr<RowGroupCollection> current_collection,
	                   optional_ptr<OptimisticDataWriter> writer = nullptr,
	                   optional_ptr<bool> written_to_disk = nullptr) {
		if (batch_index < min_batch_index) {
			throw InternalException(
			    "Batch index of the added collection (%llu) is smaller than the min batch index (%llu)", batch_index,
			    min_batch_index);
		}
		auto new_count = current_collection->GetTotalRows();
		auto batch_type =
		    new_count < RowGroup::ROW_GROUP_SIZE ? RowGroupBatchType::NOT_FLUSHED : RowGroupBatchType::FLUSHED;
		if (batch_type == RowGroupBatchType::FLUSHED && writer) {
			writer->WriteLastRowGroup(*current_collection);
		}
		optional_idx merged_batch_index;
		vector<unique_ptr<RowGroupCollection>> merge_collections;
		{
			lock_guard<mutex> l(lock);
			insert_count += new_count;

			// add the collection to the batch index
			RowGroupBatchEntry new_entry(batch_index, std::move(current_collection), batch_type);

			auto it = std::lower_bound(
			    collections.begin(), collections.end(), new_entry,
			    [&](const RowGroupBatchEntry &a, const RowGroupBatchEntry &b) { return a.batch_idx < b.batch_idx; });
			if (it != collections.end() && it->batch_idx == new_entry.batch_idx) {
				throw InternalException(
				    "PhysicalBatchInsert::AddCollection error: batch index %d is present in multiple "
				    "collections. This occurs when "
				    "batch indexes are not uniquely distributed over threads",
				    batch_index);
			}
			collections.insert(it, std::move(new_entry));
			if (writer) {
				FindMergeCollections(min_batch_index, merged_batch_index, merge_collections);
			}
		}
		if (!merge_collections.empty()) {
			// merge together the collections
			D_ASSERT(writer);
			auto final_collection = MergeCollections(context, std::move(merge_collections), *writer);
			if (written_to_disk) {
				*written_to_disk = true;
			}
			// add the merged-together collection to the set of batch indexes
			{
				lock_guard<mutex> l(lock);
				RowGroupBatchEntry new_entry(merged_batch_index.GetIndex(), std::move(final_collection),
				                             RowGroupBatchType::FLUSHED);
				auto it = std::lower_bound(collections.begin(), collections.end(), new_entry,
				                           [&](const RowGroupBatchEntry &a, const RowGroupBatchEntry &b) {
					                           return a.batch_idx < b.batch_idx;
				                           });
				if (it->batch_idx != merged_batch_index.GetIndex()) {
					throw InternalException("Merged batch index was no longer present in collection");
				}
				it->collection = std::move(new_entry.collection);
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
	optional_ptr<OptimisticDataWriter> writer;
	bool written_to_disk;

	void CreateNewCollection(DuckTableEntry &table, const vector<LogicalType> &insert_types) {
		auto &table_info = table.GetStorage().info;
		auto &block_manager = TableIOManager::Get(table.GetStorage()).GetBlockManagerForRowData();
		current_collection = make_uniq<RowGroupCollection>(table_info, block_manager, insert_types, MAX_ROW_ID);
		current_collection->InitializeEmpty();
		current_collection->InitializeAppend(current_append_state);
		written_to_disk = false;
	}
};

unique_ptr<GlobalSinkState> PhysicalBatchInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<TableCatalogEntry> table;
	if (info) {
		// CREATE TABLE AS
		D_ASSERT(!insert_table);
		auto &catalog = schema->catalog;
		table = (TableCatalogEntry *)catalog
		            .CreateTable(catalog.GetCatalogTransaction(context), *schema.get_mutable(), *info)
		            .get();
	} else {
		D_ASSERT(insert_table);
		D_ASSERT(insert_table->IsDuckTable());
		table = insert_table.get_mutable();
	}
	auto result = make_uniq<BatchInsertGlobalState>(table->Cast<DuckTableEntry>());
	return std::move(result);
}

unique_ptr<LocalSinkState> PhysicalBatchInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BatchInsertLocalState>(context.client, insert_types, bound_defaults);
}

void PhysicalBatchInsert::NextBatch(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p) const {
	auto &gstate = state.Cast<BatchInsertGlobalState>();
	auto &lstate = lstate_p.Cast<BatchInsertLocalState>();

	auto &table = gstate.table;
	auto batch_index = lstate.partition_info.batch_index.GetIndex();
	if (lstate.current_collection) {
		if (lstate.current_index == batch_index) {
			throw InternalException("NextBatch called with the same batch index?");
		}
		// batch index has changed: move the old collection to the global state and create a new collection
		TransactionData tdata(0, 0);
		lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
		gstate.AddCollection(context.client, lstate.current_index, lstate.partition_info.min_batch_index.GetIndex(),
		                     std::move(lstate.current_collection), lstate.writer, &lstate.written_to_disk);
		lstate.CreateNewCollection(table, insert_types);
	}
	lstate.current_index = batch_index;
}

SinkResultType PhysicalBatchInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BatchInsertGlobalState>();
	auto &lstate = input.local_state.Cast<BatchInsertLocalState>();

	auto &table = gstate.table;
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	auto batch_index = lstate.partition_info.batch_index.GetIndex();
	if (!lstate.current_collection) {
		lock_guard<mutex> l(gstate.lock);
		// no collection yet: create a new one
		lstate.CreateNewCollection(table, insert_types);
		lstate.writer = &table.GetStorage().CreateOptimisticWriter(context.client);
	} else if (lstate.current_index != batch_index) {
		throw InternalException("Current batch differs from batch - but NextBatch was not called!?");
	}
	lstate.current_index = batch_index;

	table.GetStorage().VerifyAppendConstraints(table, context.client, lstate.insert_chunk);

	auto new_row_group = lstate.current_collection->Append(lstate.insert_chunk, lstate.current_append_state);
	if (new_row_group) {
		// we have already written to disk - flush the next row group as well
		lstate.writer->WriteNewRowGroup(*lstate.current_collection);
		lstate.written_to_disk = true;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchInsert::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                  LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<BatchInsertGlobalState>();
	auto &lstate = lstate_p.Cast<BatchInsertLocalState>();
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this, lstate.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);

	if (!lstate.current_collection) {
		return;
	}

	if (lstate.current_collection->GetTotalRows() > 0) {
		TransactionData tdata(0, 0);
		lstate.current_collection->FinalizeAppend(tdata, lstate.current_append_state);
		gstate.AddCollection(context.client, lstate.current_index, lstate.partition_info.min_batch_index.GetIndex(),
		                     std::move(lstate.current_collection));
	}
	{
		lock_guard<mutex> l(gstate.lock);
		gstate.table.GetStorage().FinalizeOptimisticWriter(context.client, *lstate.writer);
	}
}

SinkFinalizeType PhysicalBatchInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<BatchInsertGlobalState>();

	// in the finalize, do a final pass over all of the collections we created and try to merge smaller collections
	// together
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
	storage.FinalizeOptimisticWriter(context, writer);

	// finally, merge the row groups into the local storage
	for (auto &collection : final_collections) {
		storage.LocalMerge(context, *collection);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalBatchInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<BatchInsertGlobalState>();

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));

	return SourceResultType::FINISHED;
}

} // namespace duckdb
