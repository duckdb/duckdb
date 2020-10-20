#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/storage/table/morsel_info.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
using namespace std;

LocalTableStorage::LocalTableStorage(DataTable &table) : table(table) {
	Clear();
}

LocalTableStorage::~LocalTableStorage() {
}

void LocalTableStorage::InitializeScan(LocalScanState &state, TableFilterSet *table_filters) {
	if (collection.chunks.size() == 0) {
		// nothing to scan
		return;
	}
	state.SetStorage(this);

	state.chunk_index = 0;
	state.max_index = collection.chunks.size() - 1;
	state.last_chunk_count = collection.chunks.back()->size();
	state.table_filters = table_filters;
}

LocalScanState::~LocalScanState() {
	SetStorage(nullptr);
}

void LocalScanState::SetStorage(LocalTableStorage *new_storage) {
	if (storage != nullptr) {
		assert(storage->active_scans > 0);
		storage->active_scans--;
	}
	storage = new_storage;
	if (storage) {
		storage->active_scans++;
	}
}

void LocalTableStorage::Clear() {
	collection.chunks.clear();
	collection.count = 0;
	deleted_entries.clear();
	indexes.clear();
	deleted_rows = 0;
	for (auto &index : table.info->indexes) {
		assert(index->type == IndexType::ART);
		auto &art = (ART &)*index;
		if (art.is_unique) {
			// unique index: create a local ART index that maintains the same unique constraint
			vector<unique_ptr<Expression>> unbound_expressions;
			for (auto &expr : art.unbound_expressions) {
				unbound_expressions.push_back(expr->Copy());
			}
			indexes.push_back(make_unique<ART>(art.column_ids, move(unbound_expressions), true));
		}
	}
}

void LocalStorage::InitializeScan(DataTable *table, LocalScanState &state, TableFilterSet *table_filters) {
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		// no local storage for table: set scan to nullptr
		state.SetStorage(nullptr);
		return;
	}
	auto storage = entry->second.get();
	storage->InitializeScan(state, table_filters);
}

void LocalStorage::Scan(LocalScanState &state, const vector<column_t> &column_ids, DataChunk &result) {
	auto storage = state.GetStorage();
	if (!storage || state.chunk_index > state.max_index) {
		// nothing left to scan
		result.Reset();
		return;
	}
	auto &chunk = *storage->collection.chunks[state.chunk_index];
	idx_t chunk_count = state.chunk_index == state.max_index ? state.last_chunk_count : chunk.size();
	idx_t count = chunk_count;

	// first create a selection vector from the deleted entries (if any)
	SelectionVector valid_sel(STANDARD_VECTOR_SIZE);
	auto entry = storage->deleted_entries.find(state.chunk_index);
	if (entry != storage->deleted_entries.end()) {
		// deleted entries! create a selection vector
		auto deleted = entry->second.get();
		idx_t new_count = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!deleted[i]) {
				valid_sel.set_index(new_count++, i);
			}
		}
		if (new_count == 0 && count > 0) {
			// all entries in this chunk were deleted: continue to next chunk
			state.chunk_index++;
			Scan(state, column_ids, result);
			return;
		}
		count = new_count;
	}

	SelectionVector sel;
	if (count != chunk_count) {
		sel.Initialize(valid_sel);
	} else {
		sel.Initialize(FlatVector::IncrementalSelectionVector);
	}
	// now scan the vectors of the chunk
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto id = column_ids[i];
		if (id == COLUMN_IDENTIFIER_ROW_ID) {
			// row identifier: return a sequence of rowids starting from MAX_ROW_ID plus the row offset in the chunk
			result.data[i].Sequence(MAX_ROW_ID + state.chunk_index * STANDARD_VECTOR_SIZE, 1);
		} else {
			result.data[i].Reference(chunk.data[id]);
		}
		idx_t approved_tuple_count = count;
		if (state.table_filters) {
			auto column_filters = state.table_filters->filters.find(i);
			if (column_filters != state.table_filters->filters.end()) {
				//! We have filters to apply here
				for (auto &column_filter : column_filters->second) {
					nullmask_t nullmask = FlatVector::Nullmask(result.data[i]);
					UncompressedSegment::filterSelection(sel, result.data[i], column_filter, approved_tuple_count,
					                                     nullmask);
				}
				count = approved_tuple_count;
			}
		}
	}
	if (count == 0) {
		// all entries in this chunk were filtered:: Continue on next chunk
		state.chunk_index++;
		Scan(state, column_ids, result);
		return;
	}
	if (count == chunk_count) {
		result.SetCardinality(count);
	} else {
		result.Slice(sel, count);
	}
	state.chunk_index++;
}

void LocalStorage::Append(DataTable *table, DataChunk &chunk) {
	auto entry = table_storage.find(table);
	LocalTableStorage *storage;
	if (entry == table_storage.end()) {
		auto new_storage = make_unique<LocalTableStorage>(*table);
		storage = new_storage.get();
		table_storage.insert(make_pair(table, move(new_storage)));
	} else {
		storage = entry->second.get();
	}
	// append to unique indices (if any)
	if (storage->indexes.size() > 0) {
		idx_t base_id = MAX_ROW_ID + storage->collection.count;

		// first generate the vector of row identifiers
		Vector row_ids(LOGICAL_ROW_TYPE);
		VectorOperations::GenerateSequence(row_ids, chunk.size(), base_id, 1);

		// now append the entries to the indices
		for (auto &index : storage->indexes) {
			if (!index->Append(chunk, row_ids)) {
				throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
			}
		}
	}
	//! Append to the chunk
	storage->collection.Append(chunk);
	if (storage->active_scans == 0 && storage->collection.count >= MorselInfo::MORSEL_SIZE) {
		// flush to base storage
		Flush(*table, *storage);
	}
}

LocalTableStorage *LocalStorage::GetStorage(DataTable *table) {
	auto entry = table_storage.find(table);
	assert(entry != table_storage.end());
	return entry->second.get();
}

static idx_t GetChunk(Vector &row_ids) {
	auto ids = FlatVector::GetData<row_t>(row_ids);
	auto first_id = ids[0] - MAX_ROW_ID;

	return first_id / STANDARD_VECTOR_SIZE;
}

void LocalStorage::Delete(DataTable *table, Vector &row_ids, idx_t count) {
	auto storage = GetStorage(table);
	// figure out the chunk from which these row ids came
	idx_t chunk_idx = GetChunk(row_ids);
	assert(chunk_idx < storage->collection.chunks.size());

	// get a pointer to the deleted entries for this chunk
	bool *deleted;
	auto entry = storage->deleted_entries.find(chunk_idx);
	if (entry == storage->deleted_entries.end()) {
		// nothing deleted yet, add the deleted entries
		auto del_entries = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
		memset(del_entries.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
		deleted = del_entries.get();
		storage->deleted_entries.insert(make_pair(chunk_idx, move(del_entries)));
	} else {
		deleted = entry->second.get();
	}
	storage->deleted_rows += count;

	// now actually mark the entries as deleted in the deleted vector
	idx_t base_index = MAX_ROW_ID + chunk_idx * STANDARD_VECTOR_SIZE;

	auto ids = FlatVector::GetData<row_t>(row_ids);
	for (idx_t i = 0; i < count; i++) {
		auto id = ids[i] - base_index;
		deleted[id] = true;
	}
}

template <class T>
static void update_data(Vector &data_vector, Vector &update_vector, Vector &row_ids, idx_t count, idx_t base_index) {
	VectorData udata;
	update_vector.Orrify(count, udata);

	auto target = FlatVector::GetData<T>(data_vector);
	auto &nullmask = FlatVector::Nullmask(data_vector);
	auto ids = FlatVector::GetData<row_t>(row_ids);
	auto updates = (T *)udata.data;

	for (idx_t i = 0; i < count; i++) {
		auto uidx = udata.sel->get_index(i);

		auto id = ids[i] - base_index;
		target[id] = updates[uidx];
		nullmask[id] = (*udata.nullmask)[uidx];
	}
}

static void update_chunk(Vector &data, Vector &updates, Vector &row_ids, idx_t count, idx_t base_index) {
	assert(data.type == updates.type);
	assert(row_ids.type == LOGICAL_ROW_TYPE);

	switch (data.type.InternalType()) {
	case PhysicalType::INT8:
		update_data<int8_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::INT16:
		update_data<int16_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::INT32:
		update_data<int32_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::INT64:
		update_data<int64_t>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::FLOAT:
		update_data<float>(data, updates, row_ids, count, base_index);
		break;
	case PhysicalType::DOUBLE:
		update_data<double>(data, updates, row_ids, count, base_index);
		break;
	default:
		throw Exception("Unsupported type for in-place update");
	}
}

void LocalStorage::Update(DataTable *table, Vector &row_ids, vector<column_t> &column_ids, DataChunk &data) {
	auto storage = GetStorage(table);
	// figure out the chunk from which these row ids came
	idx_t chunk_idx = GetChunk(row_ids);
	assert(chunk_idx < storage->collection.chunks.size());

	idx_t base_index = MAX_ROW_ID + chunk_idx * STANDARD_VECTOR_SIZE;

	// now perform the actual update
	auto &chunk = *storage->collection.chunks[chunk_idx];
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto col_idx = column_ids[i];
		update_chunk(chunk.data[col_idx], data.data[i], row_ids, data.size(), base_index);
	}
}

template <class T> bool LocalStorage::ScanTableStorage(DataTable &table, LocalTableStorage &storage, T &&fun) {
	vector<column_t> column_ids;
	for (idx_t i = 0; i < table.types.size(); i++) {
		column_ids.push_back(i);
	}

	DataChunk chunk;
	chunk.Initialize(table.types);

	// initialize the scan
	LocalScanState state;
	storage.InitializeScan(state);

	while (true) {
		Scan(state, column_ids, chunk);
		if (chunk.size() == 0) {
			return true;
		}
		if (!fun(chunk)) {
			return false;
		}
	}
}

void LocalStorage::Flush(DataTable &table, LocalTableStorage &storage) {
	if (storage.collection.count == 0) {
		return;
	}
	idx_t append_count = storage.collection.count - storage.deleted_rows;
	TableAppendState append_state;
	table.InitializeAppend(transaction, append_state, append_count);

	bool constraint_violated = false;
	ScanTableStorage(table, storage, [&](DataChunk &chunk) -> bool {
		// append this chunk to the indexes of the table
		if (!table.AppendToIndexes(append_state, chunk, append_state.current_row)) {
			constraint_violated = true;
			return false;
		}
		// append to base table
		table.Append(transaction, chunk, append_state);
		return true;
	});
	if (constraint_violated) {
		// need to revert the append
		row_t current_row = append_state.row_start;
		// remove the data from the indexes, if there are any indexes
		ScanTableStorage(table, storage, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			table.RemoveFromIndexes(append_state, chunk, current_row);

			current_row += chunk.size();
			if (current_row >= append_state.current_row) {
				// finished deleting all rows from the index: abort now
				return false;
			}
			return true;
		});
		table.RevertAppendInternal(append_state.row_start, append_count);
		storage.Clear();
		throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
	}
	storage.Clear();
	transaction.PushAppend(&table, append_state.row_start, append_count);
}

void LocalStorage::Commit(LocalStorage::CommitState &commit_state, Transaction &transaction, WriteAheadLog *log,
                          transaction_t commit_id) {
	// commit local storage, iterate over all entries in the table storage map
	for (auto &entry : table_storage) {
		auto table = entry.first;
		auto storage = entry.second.get();
		Flush(*table, *storage);
	}
	// finished commit: clear local storage
	table_storage.clear();
}

void LocalStorage::AddColumn(DataTable *old_dt, DataTable *new_dt, ColumnDefinition &new_column,
                             Expression *default_value) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	// take over the storage from the old entry
	auto new_storage = move(entry->second);

	// now add the new column filled with the default value to all chunks
	auto new_column_type = new_column.type;
	ExpressionExecutor executor;
	DataChunk dummy_chunk;
	if (default_value) {
		executor.AddExpression(*default_value);
	}

	new_storage->collection.types.push_back(new_column_type);
	for (idx_t chunk_idx = 0; chunk_idx < new_storage->collection.chunks.size(); chunk_idx++) {
		auto &chunk = new_storage->collection.chunks[chunk_idx];
		Vector result(new_column_type);
		if (default_value) {
			dummy_chunk.SetCardinality(chunk->size());
			executor.ExecuteExpression(dummy_chunk, result);
		} else {
			FlatVector::Nullmask(result).set();
		}
		chunk->data.push_back(move(result));
	}

	table_storage.erase(entry);
	table_storage[new_dt] = move(new_storage);
}

void LocalStorage::ChangeType(DataTable *old_dt, DataTable *new_dt, idx_t changed_idx, LogicalType target_type,
                              vector<column_t> bound_columns, Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	auto entry = table_storage.find(old_dt);
	if (entry == table_storage.end()) {
		return;
	}
	throw NotImplementedException("FIXME: ALTER TYPE with transaction local data not currently supported");
}

} // namespace duckdb
