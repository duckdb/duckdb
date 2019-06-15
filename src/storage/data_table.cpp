#include "storage/data_table.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/types/static_vector.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "planner/constraints/list.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

DataTable::DataTable(StorageManager &storage, string schema, string table, vector<TypeId> types_)
    : cardinality(0), schema(schema), table(table), types(types_), serializer(types), storage(storage) {
	index_t accumulative_size = 0;
	for (index_t i = 0; i < types.size(); i++) {
		accumulative_tuple_size.push_back(accumulative_size);
		accumulative_size += GetTypeIdSize(types[i]);
	}
	tuple_size = accumulative_size;
	// create empty statistics for the table
	statistics = unique_ptr<ColumnStatistics[]>(new ColumnStatistics[types.size()]);
	// and an empty column chunk for each column
	columns = unique_ptr<SegmentTree[]>(new SegmentTree[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		columns[i].AppendSegment(make_unique<ColumnSegment>(0));
	}
	// initialize the table with an empty storage chunk
	AppendStorageChunk(0);
}

StorageChunk *DataTable::AppendStorageChunk(index_t start) {
	auto chunk = make_unique<StorageChunk>(*this, start);
	auto chunk_pointer = chunk.get();
	// set the columns of the chunk
	chunk->columns = unique_ptr<ColumnPointer[]>(new ColumnPointer[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		chunk->columns[i].segment = (ColumnSegment *)columns[i].nodes.back().node;
		chunk->columns[i].offset = chunk->columns[i].segment->count;
	}
	storage_tree.AppendSegment(move(chunk));
	return chunk_pointer;
}

StorageChunk *DataTable::GetChunk(index_t row_number) {
	return (StorageChunk *)storage_tree.GetSegment(row_number);
}

void DataTable::AppendVector(index_t column_index, Vector &data, index_t offset, index_t count) {
	// get the segment to append to
	auto segment = (ColumnSegment *)columns[column_index].GetLastSegment();
	// append the data from the vector
	// first check how much we can append to the column segment
	index_t type_size = GetTypeIdSize(types[column_index]);
	index_t start_position = segment->offset;
	data_ptr_t target = segment->GetData() + start_position;
	index_t elements_to_copy = std::min((BLOCK_SIZE - start_position) / type_size, count);
	if (elements_to_copy > 0) {
		// we can fit elements in the current column segment: copy them there
		VectorOperations::CopyToStorage(data, target, offset, elements_to_copy);
		offset += elements_to_copy;
		segment->count += elements_to_copy;
		segment->offset += elements_to_copy * type_size;
	}
	if (elements_to_copy < count) {
		// we couldn't fit everything we wanted in the original column segment
		// create a new one
		auto column_segment = make_unique<ColumnSegment>(segment->start + segment->count);
		columns[column_index].AppendSegment(move(column_segment));
		// now try again
		AppendVector(column_index, data, offset, count - elements_to_copy);
	}
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, string &col_name) {
	if (VectorOperations::HasNull(vector)) {
		throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name.c_str(), col_name.c_str());
	}
}

static void VerifyCheckConstraint(TableCatalogEntry &table, Expression &expr, DataChunk &chunk) {
	ExpressionExecutor executor(chunk);
	Vector result(TypeId::INTEGER, true, false);
	try {
		executor.ExecuteExpression(expr, result);
	} catch (Exception &ex) {
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name.c_str(), ex.what());
	} catch (...) {
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name.c_str());
	}

	int *dataptr = (int *)result.data;
	for (index_t i = 0; i < result.count; i++) {
		index_t index = result.sel_vector ? result.sel_vector[i] : i;
		if (!result.nullmask[index] && dataptr[index] == 0) {
			throw ConstraintException("CHECK constraint failed: %s", table.name.c_str());
		}
	}
}

static void VerifyUniqueConstraint(TableCatalogEntry &table, unordered_set<index_t> &keys, DataChunk &chunk) {
	// not implemented for multiple keys
	assert(keys.size() == 1);
	// check if the columns are unique
	for (auto &key : keys) {
		if (!VectorOperations::Unique(chunk.data[key])) {
			throw ConstraintException("duplicate key value violates primary key or unique constraint");
		}
	}
}

void DataTable::VerifyAppendConstraints(TableCatalogEntry &table, DataChunk &chunk) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			VerifyNotNullConstraint(table, chunk.data[not_null.index], table.columns[not_null.index].name);
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			VerifyCheckConstraint(table, *check.expression, chunk);
			break;
		}
		case ConstraintType::UNIQUE: {
			// we check these constraint in the unique index
			auto &unique = *reinterpret_cast<BoundUniqueConstraint *>(constraint.get());
			VerifyUniqueConstraint(table, unique.keys, chunk);
			break;
		}
		case ConstraintType::FOREIGN_KEY:
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

void DataTable::AppendToIndexes(DataChunk &chunk, row_t row_start) {
	if (indexes.size() == 0) {
		return;
	}
	// first generate the vector of row identifiers
	StaticVector<row_t> row_identifiers;
	row_identifiers.sel_vector = chunk.sel_vector;
	row_identifiers.count = chunk.size();
	VectorOperations::GenerateSequence(row_identifiers, row_start);

	index_t failed_index = INVALID_INDEX;
	// now append the entries to the indices
	for (index_t i = 0; i < indexes.size(); i++) {
		if (!indexes[i]->Append(chunk, row_identifiers)) {
			failed_index = i;
			break;
		}
	}
	if (failed_index != INVALID_INDEX) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)
		for (index_t i = 0; i < failed_index; i++) {
			indexes[i]->Delete(chunk, row_identifiers);
		}
		throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
	}
}

void DataTable::Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	if (chunk.column_count != table.columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	VerifyAppendConstraints(table, chunk);

	StringHeap heap;
	chunk.MoveStringsToHeap(heap);

	row_t row_start;
	{
		// ready to append: obtain an exclusive lock on the last segment
		lock_guard<mutex> tree_lock(storage_tree.node_lock);
		auto last_chunk = (StorageChunk *)storage_tree.nodes.back().node;
		auto lock = last_chunk->lock.GetExclusiveLock();
		assert(!last_chunk->next);

		// get the start row_id of the chunk
		row_start = last_chunk->start + last_chunk->count;

		// Append the entries to the indexes, we do this first because this might fail in case of unique index conflicts
		AppendToIndexes(chunk, row_start);

		// update the statistics with the new data
		for (index_t i = 0; i < types.size(); i++) {
			statistics[i].Update(chunk.data[i]);
		}

		Transaction &transaction = context.ActiveTransaction();
		index_t remainder = chunk.size();
		index_t offset = 0;
		while (remainder > 0) {
			index_t to_copy = min(STORAGE_CHUNK_SIZE - last_chunk->count, remainder);
			if (to_copy > 0) {
				// push deleted entries into the undo buffer
				transaction.PushDeletedEntries(last_chunk->count, to_copy, last_chunk,
				                               last_chunk->version_pointers + last_chunk->count);
				// now insert the elements into the column segments
				for (index_t i = 0; i < chunk.column_count; i++) {
					AppendVector(i, chunk.data[i], offset, to_copy);
				}
				// set the is_dirty flags in the chunk
				last_chunk->SetDirtyFlag(last_chunk->count, to_copy, true);
				// now increase the count of the chunk
				last_chunk->count += to_copy;
				offset += to_copy;
				remainder -= to_copy;
			}
			if (remainder > 0) {
				last_chunk = AppendStorageChunk(last_chunk->start + last_chunk->count);
			}
		}

		// after an append move the strings to the chunk
		last_chunk->string_heap.MergeHeap(heap);
	}
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
void DataTable::Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers) {
	assert(row_identifiers.type == ROW_TYPE);
	if (row_identifiers.count == 0) {
		return;
	}

	Transaction &transaction = context.ActiveTransaction();

	auto ids = (row_t *)row_identifiers.data;
	auto sel_vector = row_identifiers.sel_vector;
	auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];
	// first find the chunk the row ids belong to
	auto chunk = GetChunk(first_id);

	// get an exclusive lock on the chunk
	auto lock = chunk->lock.GetExclusiveLock();
	// no constraints are violated
	// now delete the entries
	VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
		auto id = ids[i] - chunk->start;
		// assert that all ids in the vector belong to the same storage
		// chunk
		assert(id < chunk->count);
		// check for conflicts
		auto version = chunk->version_pointers[id];
		if (version) {
			if (version->version_number >= TRANSACTION_ID_START &&
			    version->version_number != transaction.transaction_id) {
				throw TransactionException("Conflict on tuple deletion!");
			}
		}
		// no conflict, move the current tuple data into the undo buffer
		transaction.PushTuple(UndoFlags::DELETE_TUPLE, id, chunk);
		// mark the segment of the chunk as dirty
		chunk->SetDirtyFlag(id, 1, true);
		// and set the deleted flag
		chunk->deleted[id] = true;
	});
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(TableCatalogEntry &table, vector<column_t> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	auto types = table.GetTypes();
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i]].Reference(chunk.data[i]);
		mock_chunk.sel_vector = mock_chunk.data[column_ids[i]].sel_vector;
	}
	mock_chunk.data[0].count = chunk.size();
}

static bool CreateMockChunk(TableCatalogEntry &table, vector<column_t> &column_ids,
                            unordered_set<column_t> &desired_column_ids, DataChunk &chunk, DataChunk &mock_chunk) {
	index_t found_columns = 0;
	// check whether the desired columns are present in the UPDATE clause
	for (column_t i = 0; i < column_ids.size(); i++) {
		if (desired_column_ids.find(column_ids[i]) != desired_column_ids.end()) {
			found_columns++;
		}
	}
	if (found_columns == 0) {
		// no columns were found: no need to check the constraint again
		return false;
	}
	if (found_columns != desired_column_ids.size()) {
		// FIXME: not all columns in UPDATE clause are present!
		// this should not be triggered at all as the binder should add these columns
		throw NotImplementedException(
		    "Not all columns required for the CHECK constraint are present in the UPDATED chunk!");
	}
	// construct a mock DataChunk
	CreateMockChunk(table, column_ids, chunk, mock_chunk);
	return true;
}

void DataTable::VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk, vector<column_t> &column_ids) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			// check if the constraint is in the list of column_ids
			for (index_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i] == not_null.index) {
					// found the column id: check the data in
					VerifyNotNullConstraint(table, chunk.data[i], table.columns[not_null.index].name);
					break;
				}
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());

			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, check.bound_columns, chunk, mock_chunk)) {
				VerifyCheckConstraint(table, *check.expression, mock_chunk);
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			// we check these constraint in the unique index
			auto &unique = *reinterpret_cast<BoundUniqueConstraint *>(constraint.get());
			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, unique.keys, chunk, mock_chunk)) {
				VerifyUniqueConstraint(table, unique.keys, mock_chunk);
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY:
			break;
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

void DataTable::UpdateIndexes(TableCatalogEntry &table, vector<column_t> &column_ids, DataChunk &updates,
                              Vector &row_identifiers) {
	if (indexes.size() == 0) {
		return;
	}
	// first create a mock chunk to be used in the index appends
	DataChunk mock_chunk;
	CreateMockChunk(table, column_ids, updates, mock_chunk);

	index_t failed_index = INVALID_INDEX;
	// now insert the updated values into the index
	for (index_t i = 0; i < indexes.size(); i++) {
		// first check if the index is affected by the update
		if (!indexes[i]->IndexIsUpdated(column_ids)) {
			continue;
		}
		// if it is, we append the data to the index
		if (!indexes[i]->Append(mock_chunk, row_identifiers)) {
			failed_index = i;
			break;
		}
	}
	if (failed_index != INVALID_INDEX) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)
		for (index_t i = 0; i < failed_index; i++) {
			if (indexes[i]->IndexIsUpdated(column_ids)) {
				indexes[i]->Delete(mock_chunk, row_identifiers);
			}
		}
		throw ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicated key");
	}
}

void DataTable::Update(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers,
                       vector<column_t> &column_ids, DataChunk &updates) {
	assert(row_identifiers.type == ROW_TYPE);
	updates.Verify();
	if (row_identifiers.count == 0) {
		return;
	}

	// first verify that no constraints are violated
	VerifyUpdateConstraints(table, updates, column_ids);

	// move strings to a temporary heap
	StringHeap heap;
	updates.MoveStringsToHeap(heap);

	{
		// now perform the actual update
		Transaction &transaction = context.ActiveTransaction();

		auto ids = (row_t *)row_identifiers.data;
		auto sel_vector = row_identifiers.sel_vector;
		auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];
		// first find the chunk the row ids belong to
		auto chunk = GetChunk(first_id);

		// get an exclusive lock on the chunk
		auto lock = chunk->lock.GetExclusiveLock();

		// now update the entries
		// first check for any conflicts before we insert anything into the undo buffer
		// we check for any conflicts in ALL tuples first before inserting anything into the undo buffer
		// to make sure that we complete our entire update transaction after we do
		// this prevents inconsistencies after rollbacks, etc...
		VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
			auto id = ids[i] - chunk->start;
			// assert that all ids in the vector belong to the same chunk
			assert(id < chunk->count);
			// check for version conflicts
			auto version = chunk->version_pointers[id];
			if (version) {
				if (version->version_number >= TRANSACTION_ID_START &&
				    version->version_number != transaction.transaction_id) {
					throw TransactionException("Conflict on tuple update!");
				}
			}
		});

		// now we update any indexes, we do this before inserting anything into the undo buffer
		UpdateIndexes(table, column_ids, updates, row_identifiers);

		// now we know thre are no conflicts, move the tuples into the undo buffer and mark the chunk as dirty
		VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
			auto id = ids[i] - chunk->start;
			// move the current tuple data into the undo buffer
			transaction.PushTuple(UndoFlags::UPDATE_TUPLE, id, chunk);
			// mark the segment as dirty in the chunk
			chunk->SetDirtyFlag(id, 1, true);
		});

		// now update the columns in the base table
		for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
			auto column_id = column_ids[col_idx];
			auto size = GetTypeIdSize(updates.data[col_idx].type);

			Vector *update_vector = &updates.data[col_idx];
			Vector null_vector;
			if (update_vector->nullmask.any()) {
				// has NULL values in the nullmask
				// copy them to a temporary vector
				null_vector.Initialize(update_vector->type, false);
				null_vector.count = update_vector->count;
				VectorOperations::CopyToStorage(*update_vector, null_vector.data);
				update_vector = &null_vector;
			}

			VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
				auto dataptr = chunk->GetPointerToRow(column_ids[col_idx], ids[i]);
				auto update_index = update_vector->sel_vector ? update_vector->sel_vector[k] : k;
				memcpy(dataptr, update_vector->data + update_index * size, size);
			});

			// update the statistics with the new data
			statistics[column_id].Update(updates.data[col_idx]);
		}
		// after a successful update move the strings into the chunk
		chunk->string_heap.MergeHeap(heap);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::RetrieveVersionedData(DataChunk &result, const vector<column_t> &column_ids,
                                      data_ptr_t alternate_version_pointers[], index_t alternate_version_index[],
                                      index_t alternate_version_count) {
	assert(alternate_version_count > 0);
	// get data from the alternate versions for each column
	for (index_t j = 0; j < column_ids.size(); j++) {
		if (column_ids[j] == COLUMN_IDENTIFIER_ROW_ID) {
			assert(result.data[j].type == TypeId::BIGINT);
			// assign the row identifiers
			auto data = ((int64_t *)result.data[j].data) + result.data[j].count;
			for (index_t k = 0; k < alternate_version_count; k++) {
				data[k] = alternate_version_index[k];
			}
		} else {
			// grab data from the stored tuple for each column
			index_t tuple_size = GetTypeIdSize(result.data[j].type);
			auto res_data = result.data[j].data + result.data[j].count * tuple_size;
			index_t offset = accumulative_tuple_size[column_ids[j]];
			for (index_t k = 0; k < alternate_version_count; k++) {
				auto base_data = alternate_version_pointers[k] + offset;
				memcpy(res_data, base_data, tuple_size);
				res_data += tuple_size;
			}
		}
		result.data[j].count += alternate_version_count;
	}
}

void DataTable::RetrieveColumnData(Vector &result, TypeId type, ColumnPointer &pointer, index_t count) {
	index_t type_size = GetTypeIdSize(type);
	// copy data from the column storage
	while (count > 0) {
		// check how much we can copy from this column segment
		index_t to_copy = std::min(count, (BLOCK_SIZE - pointer.offset) / type_size);
		if (to_copy > 0) {
			// copy elements from the column segment
			data_ptr_t dataptr = pointer.segment->GetData() + pointer.offset;
			Vector source(type, dataptr);
			source.count = to_copy;
			// FIXME: use ::Copy instead of ::AppendFromStorage if there are no null values in this segment
			VectorOperations::AppendFromStorage(source, result);
			pointer.offset += type_size * to_copy;
			count -= to_copy;
		}
		if (count > 0) {
			// there is still chunks to copy
			// move to the next segment
			assert(pointer.segment->next);
			pointer.segment = (ColumnSegment *)pointer.segment->next.get();
			pointer.offset = 0;
		}
	}
}

void DataTable::RetrieveColumnData(Vector &result, TypeId type, ColumnPointer &pointer, index_t count,
                                   sel_t *sel_vector, index_t sel_count) {
	index_t type_size = GetTypeIdSize(type);
	if (sel_count == 0) {
		// skip this segment
		pointer.offset += type_size * count;
		return;
	}
	// copy data from the column storage
	while (count > 0) {
		// check how much we can copy from this column segment
		index_t to_copy = std::min(count, (BLOCK_SIZE - pointer.offset) / type_size);
		if (to_copy == 0) {
			// we can't copy anything from this segment, move to the next segment
			assert(pointer.segment->next);
			pointer.segment = (ColumnSegment *)pointer.segment->next.get();
			pointer.offset = 0;
		} else {
			// accesses to RetrieveColumnData should be aligned by STANDARD_VECTOR_SIZE, hence to_copy should be
			// equivalent to either count or 0
			assert(to_copy == count);
			// we can copy everything from this column segment, copy with the sel vector
			data_ptr_t dataptr = pointer.segment->GetData() + pointer.offset;
			Vector source(type, dataptr);
			source.count = sel_count;
			source.sel_vector = sel_vector;
			// FIXME: use ::Copy instead of ::AppendFromStorage if there are no null values in this segment
			VectorOperations::AppendFromStorage(source, result);
			pointer.offset += type_size * to_copy;
			count -= to_copy;
		}
	}
}

void DataTable::InitializeScan(ScanState &state) {
	state.chunk = (StorageChunk *)storage_tree.GetRootSegment();
	state.last_chunk = (StorageChunk *)storage_tree.GetLastSegment();
	state.last_chunk_count = state.last_chunk->count;
	state.columns = unique_ptr<ColumnPointer[]>(new ColumnPointer[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		state.columns[i].segment = (ColumnSegment *)columns[i].GetRootSegment();
		state.columns[i].offset = 0;
	}
	state.offset = 0;
	state.version_chain = nullptr;
}

//! Given the specified root version, fetches the version that belongs to this transaction
static VersionInformation *GetVersionInfo(Transaction &transaction, VersionInformation *version) {
	if (!version ||
	    (version->version_number == transaction.transaction_id || version->version_number < transaction.start_time)) {
		// either (1) there is no version anymore as it was cleaned up,
		// or (2) the base table data belongs to the current transaction
		// in this case use the data in the original table
		return nullptr;
	} else {
		// follow the version pointers
		while (true) {
			auto next = version->next;
			if (!next) {
				// use this version: no predecessor
				break;
			}
			if (next->version_number == transaction.transaction_id) {
				// use this version: it was created by us
				break;
			}
			if (next->version_number < transaction.start_time) {
				// use this version: it was committed by us
				break;
			}
			version = next;
		}
		return version;
	}
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
                     ScanState &state) {
	sel_t regular_entries[STANDARD_VECTOR_SIZE], version_entries[STANDARD_VECTOR_SIZE];
	index_t regular_count, version_count;
	// scan the base table
	while (state.chunk) {
		auto current_chunk = state.chunk;
		// first obtain a shared lock on the current chunk
		auto lock = current_chunk->lock.GetSharedLock();
		regular_count = version_count = 0;
		index_t end = current_chunk == state.last_chunk ? state.last_chunk_count : current_chunk->count;
		index_t scan_count = min((index_t)STANDARD_VECTOR_SIZE, end - state.offset);
		if (scan_count == 0) {
			return;
		}
		// if the segment is dirty we need to scan the version pointers and deleted flags
		if (current_chunk->IsDirty(state.offset, scan_count)) {
			// start scanning the chunk to check for deleted and version pointers
			for (index_t i = 0; i < scan_count; i++) {
				version_entries[version_count] = regular_entries[regular_count] = i;
				bool has_version = current_chunk->version_pointers[state.offset + i];
				bool is_deleted = current_chunk->deleted[state.offset + i];
				version_count += has_version;
				regular_count += !(is_deleted || has_version);
			}
			if (regular_count == scan_count) {
				// the scan was clean: set the dirty flag to false
				current_chunk->SetDirtyFlag(state.offset, scan_count, false);
			}
		} else {
			// no deleted entries or version information: just scan everything
			regular_count = scan_count;
		}

		if (regular_count < scan_count) {
			// there are versions! chase the version pointers
			data_ptr_t alternate_version_pointers[STANDARD_VECTOR_SIZE];
			index_t alternate_version_index[STANDARD_VECTOR_SIZE];
			index_t alternate_version_count = 0;

			for (index_t i = 0; i < version_count; i++) {
				auto root_info = current_chunk->version_pointers[state.offset + version_entries[i]];
				// follow the version chain for this version
				auto version_info = GetVersionInfo(transaction, root_info);
				if (!version_info) {
					// no version info available for this transaction: use base table data
					// check if entry was not deleted, if it was not deleted use the base table data
					if (!current_chunk->deleted[state.offset + version_entries[i]]) {
						regular_entries[regular_count++] = version_entries[i];
					}
				} else {
					// version info available: use the version info
					if (version_info->tuple_data) {
						alternate_version_pointers[alternate_version_count] = version_info->tuple_data;
						alternate_version_index[alternate_version_count] = state.offset + version_entries[i];
						alternate_version_count++;
					}
				}
			}
			if (alternate_version_count > 0) {
				// retrieve alternate versions, if any
				RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index,
				                      alternate_version_count);
			}
			// retrieve entries from the base table with the selection vector
			for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
				if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
					assert(result.data[col_idx].type == TypeId::BIGINT);
					auto column_indexes = ((int64_t *)result.data[col_idx].data + result.data[col_idx].count);
					for (index_t i = 0; i < regular_count; i++) {
						column_indexes[i] = current_chunk->start + state.offset + regular_entries[i];
					}
					result.data[col_idx].count += regular_count;
				} else {
					// fetch the data from the base column segments
					RetrieveColumnData(result.data[col_idx], types[column_ids[col_idx]],
					                   state.columns[column_ids[col_idx]], scan_count, regular_entries, regular_count);
				}
			}
		} else {
			// no versions or deleted tuples, simply scan the column segments
			for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
				if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
					// generate column ids
					result.data[col_idx].count = regular_count;
					VectorOperations::GenerateSequence(result.data[col_idx], current_chunk->start + state.offset, 1);
				} else {
					// fetch the data from the base column segments
					RetrieveColumnData(result.data[col_idx], types[column_ids[col_idx]],
					                   state.columns[column_ids[col_idx]], regular_count);
				}
			}
		}
		// move to the next segment
		state.offset += STANDARD_VECTOR_SIZE;
		if (state.offset >= end) {
			if (state.chunk == state.last_chunk) {
				state.chunk = nullptr;
				break;
			} else {
				state.offset = 0;
				state.chunk = (StorageChunk *)current_chunk->next.get();
			}
		}
		if (result.size() > 0) {
			return;
		}
	}
}

void DataTable::RetrieveTupleFromBaseTable(DataChunk &result, StorageChunk *chunk, vector<column_t> &column_ids,
                                           row_t row_id) {
	assert(result.size() < STANDARD_VECTOR_SIZE);
	assert((index_t)row_id >= chunk->start);
	assert(column_ids.size() == result.column_count);
	for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
			((row_t *)result.data[col_idx].data)[result.data[col_idx].count] = row_id;
			result.data[col_idx].count++;
		} else {
			assert(column_ids[col_idx] < types.size());
			// get the column segment for this entry and append it to the vector
			chunk->columns[column_ids[col_idx]].segment->AppendValue(result.data[col_idx], types[column_ids[col_idx]],
			                                                         row_id);
		}
	}
}

void DataTable::Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids,
                      Vector &row_identifiers) {
	assert(row_identifiers.type == ROW_TYPE);
	auto row_ids = (row_t *)row_identifiers.data;
	// sort the row identifiers first
	// this is done so we can minimize the amount of chunks that we lock
	sel_t sort_vector[STANDARD_VECTOR_SIZE];
	VectorOperations::Sort(row_identifiers, sort_vector);

	for (index_t i = 0; i < row_identifiers.count; i++) {
		auto row_id = row_ids[sort_vector[i]];
		auto chunk = GetChunk(row_id);
		auto lock = chunk->lock.GetSharedLock();

		assert((index_t)row_id >= chunk->start && (index_t)row_id < chunk->start + chunk->count);
		auto index = row_id - chunk->start;

		// check if this tuple is versioned
		auto root_info = chunk->version_pointers[index];
		auto version_info = GetVersionInfo(transaction, root_info);
		if (version_info) {
			if (version_info->tuple_data) {
				// tuple is versioned: retrieve the versioned data
				data_ptr_t alternate_version_pointer = version_info->tuple_data;
				index_t alternate_version_index = row_id;

				RetrieveVersionedData(result, column_ids, &alternate_version_pointer, &alternate_version_index, 1);
			}
		} else {
			if (!chunk->deleted[index]) {
				// not versioned: retrieve info from base table
				RetrieveTupleFromBaseTable(result, chunk, column_ids, row_id);
			}
		}
	}
}

void DataTable::InitializeIndexScan(IndexScanState &state) {
	InitializeScan(state);
	state.base_offset = 0;
}

void DataTable::CreateIndexScan(IndexScanState &state, vector<column_t> &column_ids, DataChunk &result) {
	index_t result_count = 0;
	while (state.chunk) {
		auto current_chunk = state.chunk;
		if (state.offset == 0 && state.base_offset == 0 && !state.version_chain) {
			// first obtain a shared lock on the current chunk, if we don't have
			// it already
			state.locks.push_back(current_chunk->lock.GetSharedLock());
		}

		while (state.base_offset < current_chunk->count) {
			// fill the result until we exhaust the tuples
			// first scan the base tuples of this chunk
			sel_t regular_entries[STANDARD_VECTOR_SIZE];
			index_t scan_count = min((index_t)STANDARD_VECTOR_SIZE, current_chunk->count - state.base_offset);
			{
				// figure out the deleted tuples
				// FIXME: we don't need to do this if the dirty flag is not set for this piece
				for (index_t i = 0; i < scan_count; i++) {
					if (!current_chunk->deleted[state.base_offset + i]) {
						regular_entries[result_count++] = i;
					}
				}
			}
			if (result_count == scan_count) {
				// no deleted tuples, use basic fetch calls
				for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
					if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
						// generate column ids
						result.data[col_idx].count = result_count;
						VectorOperations::GenerateSequence(result.data[col_idx],
						                                   current_chunk->start + state.base_offset, 1);
					} else {
						// fetch the data from the base column segments
						RetrieveColumnData(result.data[col_idx], types[column_ids[col_idx]],
						                   state.columns[column_ids[col_idx]], result_count);
					}
				}
			} else {
				// scan the tuples found in the base table
				for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
					if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
						assert(result.data[col_idx].type == TypeId::BIGINT);
						auto column_indexes = ((int64_t *)result.data[col_idx].data + result.data[col_idx].count);
						for (index_t i = 0; i < result_count; i++) {
							column_indexes[i] = current_chunk->start + state.base_offset + regular_entries[i];
						}
						result.data[col_idx].count += result_count;
					} else {
						RetrieveColumnData(result.data[col_idx], types[column_ids[col_idx]],
						                   state.columns[column_ids[col_idx]], scan_count, regular_entries,
						                   result_count);
					}
				}
			}
			state.base_offset += scan_count;
			assert(result.size() == result_count);
			if (result_count > 0) {
				return;
			}
		}
		// the base table was exhausted, now scan any remaining version chunks
		data_ptr_t alternate_version_pointers[STANDARD_VECTOR_SIZE];
		index_t alternate_version_index[STANDARD_VECTOR_SIZE];

		while (state.offset < current_chunk->count) {
			if (!state.version_chain) {
				state.version_chain = current_chunk->version_pointers[state.offset];
			}

			// now chase the version pointer, if any
			while (state.version_chain) {
				if (state.version_chain->tuple_data) {
					alternate_version_pointers[result_count] = state.version_chain->tuple_data;
					alternate_version_index[result_count] = state.offset;
					result_count++;
				}
				state.version_chain = state.version_chain->next;
				if (result_count == STANDARD_VECTOR_SIZE) {
					break;
				}
			}
			if (!state.version_chain) {
				state.offset++;
				state.version_chain = nullptr;
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				break;
			}
		}
		if (state.offset >= current_chunk->count) {
			// exceeded this chunk, move to next one
			state.chunk = (StorageChunk *)state.chunk->next.get();
			state.offset = 0;
			state.base_offset = 0;
			state.version_chain = nullptr;
		}
		if (result_count > 0) {
			// retrieve the version data, if there was any
			RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index,
			                      result_count);
			return;
		}
	}
}
