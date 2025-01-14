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

PhysicalDelete::PhysicalDelete(vector<LogicalType> types, TableCatalogEntry &tableref, DataTable &table,
                               vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t row_id_index,
                               idx_t estimated_cardinality, bool return_chunk)
    : PhysicalOperator(PhysicalOperatorType::DELETE_OPERATOR, std::move(types), estimated_cardinality),
      tableref(tableref), table(table), bound_constraints(std::move(bound_constraints)), row_id_index(row_id_index),
      return_chunk(return_chunk) {
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
	LocalAppendState delete_index_append_state;
	bool has_unique_indexes;
};

class DeleteLocalState : public LocalSinkState {
public:
	DeleteLocalState(ClientContext &context, TableCatalogEntry &table,
	                 const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
		delete_chunk.Initialize(Allocator::Get(context), table.GetTypes());
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

	auto &transaction = DuckTransaction::Get(context.client, table.db);
	auto &row_ids = chunk.data[row_id_index];

	vector<StorageIndex> column_ids;
	for (idx_t i = 0; i < table.ColumnCount(); i++) {
		column_ids.emplace_back(i);
	};
	auto fetch_state = ColumnFetchState();

	lock_guard<mutex> delete_guard(g_state.delete_lock);
	if (!return_chunk && !g_state.has_unique_indexes) {
		g_state.deleted_count += table.Delete(*l_state.delete_state, context.client, row_ids, chunk.size());
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Fetch the to-be-deleted chunk.
	l_state.delete_chunk.Reset();
	row_ids.Flatten(chunk.size());
	table.Fetch(transaction, l_state.delete_chunk, column_ids, row_ids, chunk.size(), fetch_state);

	// Append the deleted row IDs to the delete indexes.
	// If we only delete local row IDs, then the delete_chunk is empty.
	if (g_state.has_unique_indexes && l_state.delete_chunk.size() != 0) {
		auto &local_storage = LocalStorage::Get(context.client, table.db);
		auto storage = local_storage.GetStorage(table);
		storage->delete_indexes.Scan([&](Index &index) {
			if (!index.IsBound() || !index.IsUnique()) {
				return false;
			}
			auto &bound_index = index.Cast<BoundIndex>();
			auto error = bound_index.Append(l_state.delete_chunk, row_ids);
			if (error.HasError()) {
				throw InternalException("failed to update delete ART in physical delete: ", error.Message());
			}
			return false;
		});
	}

	// Append the return_chunk to the return collection.
	if (return_chunk) {
		g_state.return_collection.Append(l_state.delete_chunk);
	}

	g_state.deleted_count += table.Delete(*l_state.delete_state, context.client, row_ids, chunk.size());
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DeleteGlobalState>(context, GetTypes(), tableref, bound_constraints);
}

unique_ptr<LocalSinkState> PhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DeleteLocalState>(context.client, tableref, bound_constraints);
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

SourceResultType PhysicalDelete::GetData(ExecutionContext &context, DataChunk &chunk,
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
