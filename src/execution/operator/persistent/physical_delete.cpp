#include "duckdb/execution/operator/persistent/physical_delete.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

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

	mutex delete_lock;
	idx_t deleted_count;
	ColumnDataCollection return_collection;
	unordered_set<row_t> deleted_row_ids;
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
	}

public:
	DataChunk delete_chunk;
	unique_ptr<TableDeleteState> delete_state;
};

SinkResultType PhysicalDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &g_state = input.global_state.Cast<DeleteGlobalState>();
	auto &l_state = input.local_state.Cast<DeleteLocalState>();

	auto &row_ids = chunk.data[row_id_index];

	lock_guard<mutex> delete_guard(g_state.delete_lock);
	if (!return_chunk && !g_state.has_unique_indexes) {
		g_state.deleted_count += table.Delete(*l_state.delete_state, context.client, row_ids, chunk.size());
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto types = table.GetTypes();
	l_state.delete_chunk.Reset();
	row_ids.Flatten(chunk.size());

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

	// Append the deleted row IDs to the delete indexes.
	// If we only delete local row IDs, then the delete_chunk is empty.
	if (g_state.has_unique_indexes && l_state.delete_chunk.size() != 0) {
		auto &local_storage = LocalStorage::Get(context.client, table.db);
		auto storage = local_storage.GetStorage(table);
		IndexAppendInfo index_append_info(IndexAppendMode::IGNORE_DUPLICATES, nullptr);
		for (auto &index : storage->delete_indexes.Indexes()) {
			if (!index.IsBound() || !index.IsUnique()) {
				continue;
			}
			auto &bound_index = index.Cast<BoundIndex>();
			auto error = bound_index.Append(l_state.delete_chunk, row_ids, index_append_info);
			if (error.HasError()) {
				throw InternalException("failed to update delete ART in physical delete: ", error.Message());
			}
		}
	}

	auto deleted_count = table.Delete(*l_state.delete_state, context.client, row_ids, chunk.size());
	g_state.deleted_count += deleted_count;

	// Append the return_chunk to the return collection.
	if (return_chunk) {
		// Rows can be duplicated, so we get the chunk indexes for new row id values.
		map<row_t, idx_t> new_row_ids_deleted;
		auto flat_ids = FlatVector::GetData<row_t>(row_ids);
		for (idx_t i = 0; i < chunk.size(); i++) {
			// If the row has not been deleted previously
			// and is not a duplicate within the current chunk,
			// then we add it to new_row_ids_deleted.
			auto row_id = flat_ids[i];
			auto already_deleted = g_state.deleted_row_ids.find(row_id) != g_state.deleted_row_ids.end();
			auto newly_deleted = new_row_ids_deleted.find(row_id) != new_row_ids_deleted.end();
			if (!already_deleted && !newly_deleted) {
				new_row_ids_deleted[row_id] = i;
				g_state.deleted_row_ids.insert(row_id);
			}
		}

		D_ASSERT(new_row_ids_deleted.size() == deleted_count);
		if (deleted_count < l_state.delete_chunk.size()) {
			SelectionVector delete_sel(0, deleted_count);
			idx_t chunk_index = 0;
			for (auto &row_id_to_chunk_index : new_row_ids_deleted) {
				delete_sel.set_index(chunk_index, row_id_to_chunk_index.second);
				chunk_index++;
			}
			l_state.delete_chunk.Slice(delete_sel, deleted_count);
		}
		g_state.return_collection.Append(l_state.delete_chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
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
			g.return_collection.InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DeleteSourceState>(*this);
}

SourceResultType PhysicalDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<DeleteSourceState>();
	auto &g = sink_state->Cast<DeleteGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.deleted_count)));
		return SourceResultType::FINISHED;
	}

	g.return_collection.Scan(state.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
