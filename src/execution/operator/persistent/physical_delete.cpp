#include "duckdb/execution/operator/persistent/physical_delete.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/delete_state.hpp"

namespace duckdb {

PhysicalDelete::PhysicalDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &tableref,
                               DataTable &table, vector<unique_ptr<BoundConstraint>> bound_constraints,
                               idx_t row_id_index, idx_t estimated_cardinality, bool return_chunk,
                               vector<idx_t> return_columns)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::DELETE_OPERATOR, std::move(types), estimated_cardinality),
      tableref(tableref), table(table), bound_constraints(std::move(bound_constraints)), row_id_index(row_id_index),
      return_chunk(return_chunk), return_columns(std::move(return_columns)) {
}
//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DeleteGlobalState : public GlobalSinkState {
public:
	explicit DeleteGlobalState(ClientContext &context, const vector<LogicalType> &return_types,
	                           TableCatalogEntry &table, const vector<unique_ptr<BoundConstraint>> &bound_constraints)
	    : deleted_count(0), return_collection(context, return_types), has_unique_indexes(false) {
		// We need to append deletes to the local delete-ART.
		auto &storage = table.GetStorage();
		if (storage.HasUniqueIndexes()) {
			storage.InitializeLocalStorage(delete_index_append_state, table, context, bound_constraints);
			has_unique_indexes = true;
		}
	}

	//! Protects deleted_row_ids and return_collection Combine()
	annotated_mutex return_lock;
	//! Conservatively protect delete-index maintenance (unique indexes)
	annotated_mutex index_lock;

	atomic<idx_t> deleted_count;
	ColumnDataCollection return_collection;
	//! Global set of deleted row_ids (for cross-thread dedup in Sink; only accessed under return_lock)
	unordered_set<row_t> deleted_row_ids DUCKDB_GUARDED_BY(return_lock);
	LocalAppendState delete_index_append_state;
	bool has_unique_indexes;
};

class DeleteLocalState : public LocalSinkState {
public:
	DeleteLocalState(ClientContext &context, TableCatalogEntry &table,
	                 const vector<unique_ptr<BoundConstraint>> &bound_constraints,
	                 const vector<LogicalType> &return_types, bool return_chunk) {
		// For RETURNING: use operator's return types (may include virtual columns)
		// For non-RETURNING with indexes: use table types for index updates
		auto types = return_chunk ? return_types : table.GetTypes();
		auto initialize = vector<bool>(types.size(), false);
		delete_chunk.Initialize(Allocator::Get(context), types, initialize);

		auto &storage = table.GetStorage();
		delete_state = storage.InitializeDelete(table, context, bound_constraints);

		if (return_chunk) {
			// Collect RETURNING rows per-thread, merge into global in Combine()
			return_collection = make_uniq<ColumnDataCollection>(context, return_types);
			// Per-thread set of deleted row_ids (filters intra-thread duplicates lock-free)
			deleted_row_ids = make_uniq<unordered_set<row_t>>();
			// Reusable buffer for global-pass selection to avoid per-chunk allocation
			final_sel.Initialize(STANDARD_VECTOR_SIZE);
		}
	}

public:
	DataChunk delete_chunk;
	unique_ptr<TableDeleteState> delete_state;
	unique_ptr<ColumnDataCollection> return_collection;
	unique_ptr<unordered_set<row_t>> deleted_row_ids;
	//! Reusable selection for global row_id pass (avoids allocation in Sink hot path)
	SelectionVector final_sel;
};

SinkResultType PhysicalDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &g_state = input.global_state.Cast<DeleteGlobalState>();
	auto &l_state = input.local_state.Cast<DeleteLocalState>();

	auto &row_ids = chunk.data[row_id_index];
	row_ids.Flatten(chunk.size());

	// Fast path: no RETURNING and no unique indexes
	if (!return_chunk && !g_state.has_unique_indexes) {
		auto deleted = table.Delete(*l_state.delete_state, context.client, row_ids, chunk.size());
		g_state.deleted_count.fetch_add(deleted);
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto types = table.GetTypes();
	l_state.delete_chunk.Reset();

	SelectionVector delete_sel(chunk.size());
	idx_t delete_count = chunk.size();
	Vector delete_row_ids(row_ids);

	// If RETURNING is enabled, deduplicate row_ids: per-thread set (lock-free) then global set (batched lock).
	if (return_chunk) {
		D_ASSERT(l_state.deleted_row_ids);
		auto flat_ids = FlatVector::GetData<row_t>(row_ids);
		idx_t count = 0;
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto row_id = flat_ids[i];
			if (l_state.deleted_row_ids->insert(row_id).second) {
				delete_sel.set_index(count++, i);
			}
		}
		if (count == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		{
			annotated_lock_guard<annotated_mutex> guard(g_state.return_lock);
			idx_t final_count = 0;
			unique_ptr<SelectionVector> large_sel;
			SelectionVector *write_sel = (count <= STANDARD_VECTOR_SIZE) ? &l_state.final_sel : nullptr;
			if (!write_sel) {
				large_sel = make_uniq<SelectionVector>(chunk.size());
				write_sel = large_sel.get();
			}
			for (idx_t i = 0; i < count; i++) {
				auto orig_idx = delete_sel[i];
				auto row_id = flat_ids[orig_idx];
				if (g_state.deleted_row_ids.insert(row_id).second) {
					write_sel->set_index(final_count++, orig_idx);
				}
			}
			if (final_count == 0) {
				return SinkResultType::NEED_MORE_INPUT;
			}
			delete_count = final_count;
			if (write_sel == &l_state.final_sel) {
				delete_sel.Initialize(l_state.final_sel.data());
			} else {
				delete_sel = std::move(*large_sel);
			}
		}
		if (delete_count != chunk.size()) {
			delete_row_ids.Slice(row_ids, delete_sel, delete_count);
		}
	}

	// Use columns from the input chunk - they were passed through from the scan
	// return_columns maps storage_idx -> chunk_idx
	// For RETURNING: all columns have valid indices
	// For index-only: only indexed columns have valid indices (sparse mapping)
	D_ASSERT(!return_columns.empty() && "return_columns should always be populated for RETURNING or unique indexes");
	for (idx_t i = 0; i < table.ColumnCount(); i++) {
		if (return_columns[i] != DConstants::INVALID_INDEX) {
			// Column was passed through from the scan
			l_state.delete_chunk.data[i].Reference(chunk.data[return_columns[i]]);
		} else {
			// Column not in scan (sparse mapping for index-only case) - use NULL placeholder
			l_state.delete_chunk.data[i].Reference(Value(types[i]));
		}
	}
	// Add virtual columns (e.g., rowid) after table columns
	for (idx_t i = table.ColumnCount(); i < l_state.delete_chunk.ColumnCount(); i++) {
		l_state.delete_chunk.data[i].Reference(row_ids);
	}
	l_state.delete_chunk.SetCardinality(chunk.size());

	// Slice down to the claimed rows (RETURNING only)
	if (return_chunk && delete_count != chunk.size()) {
		l_state.delete_chunk.Slice(delete_sel, delete_count);
	}

	// Append the deleted row IDs to the delete indexes.
	// If we only delete local row IDs, then the delete_chunk is empty.
	if (g_state.has_unique_indexes && l_state.delete_chunk.size() != 0) {
		annotated_lock_guard<annotated_mutex> index_guard(g_state.index_lock);
		auto &local_storage = LocalStorage::Get(context.client, table.db);
		auto storage = local_storage.GetStorage(table);
		IndexAppendInfo index_append_info(IndexAppendMode::IGNORE_DUPLICATES, nullptr);
		for (auto &index : storage->delete_indexes.Indexes()) {
			if (!index.IsBound() || !index.IsUnique()) {
				continue;
			}
			auto &bound_index = index.Cast<BoundIndex>();
			auto error = bound_index.Append(l_state.delete_chunk, delete_row_ids, index_append_info);
			if (error.HasError()) {
				throw InternalException("failed to update delete ART in physical delete: ", error.Message());
			}
		}
	}

	auto deleted = table.Delete(*l_state.delete_state, context.client, delete_row_ids, delete_count);
	g_state.deleted_count.fetch_add(deleted);

	// Collect RETURNING rows per-thread; merge into global in Combine()
	if (return_chunk) {
		D_ASSERT(l_state.return_collection);
		D_ASSERT(deleted == delete_count);
		l_state.return_collection->Append(l_state.delete_chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalDelete::Combine(ExecutionContext &, OperatorSinkCombineInput &input) const {
	if (!return_chunk) {
		return SinkCombineResultType::FINISHED;
	}
	auto &g_state = input.global_state.Cast<DeleteGlobalState>();
	auto &l_state = input.local_state.Cast<DeleteLocalState>();
	if (!l_state.return_collection || l_state.return_collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}
	// Merge per-thread return_collection into global (deleted_row_ids is only needed during Sink)
	annotated_lock_guard<annotated_mutex> guard(g_state.return_lock);
	g_state.return_collection.Combine(*l_state.return_collection);
	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DeleteGlobalState>(context, GetTypes(), tableref, bound_constraints);
}

unique_ptr<LocalSinkState> PhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DeleteLocalState>(context.client, tableref, bound_constraints, GetTypes(), return_chunk);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DeleteSourceState : public GlobalSourceState {
public:
	explicit DeleteSourceState(const PhysicalDelete &op) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = op.sink_state->Cast<DeleteGlobalState>();
			g.return_collection.InitializeScan(global_scan_state);
			max_threads = MaxValue<idx_t>(g.return_collection.ChunkCount(), 1);
		} else {
			max_threads = 1;
		}
	}

	idx_t MaxThreads() override {
		return max_threads;
	}

	ColumnDataParallelScanState global_scan_state;
	idx_t max_threads;
};

class DeleteLocalSourceState : public LocalSourceState {
public:
	ColumnDataLocalScanState local_scan_state;
};

unique_ptr<GlobalSourceState> PhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DeleteSourceState>(*this);
}

unique_ptr<LocalSourceState> PhysicalDelete::GetLocalSourceState(ExecutionContext &, GlobalSourceState &) const {
	return make_uniq<DeleteLocalSourceState>();
}

SourceResultType PhysicalDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<DeleteSourceState>();
	auto &g = sink_state->Cast<DeleteGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.deleted_count.load())));
		return SourceResultType::FINISHED;
	}

	auto &lstate = input.local_state.Cast<DeleteLocalSourceState>();
	g.return_collection.Scan(state.global_scan_state, lstate.local_scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
