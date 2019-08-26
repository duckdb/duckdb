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
#include "storage/table/transient_segment.hpp"

#include "transaction/version_info.hpp"

using namespace duckdb;
using namespace std;

DataTable::DataTable(StorageManager &storage, string schema, string table, vector<TypeId> types_,
                     unique_ptr<vector<unique_ptr<PersistentSegment>>[]> data)
    : cardinality(0), schema(schema), table(table), types(types_), storage(storage) {
	index_t accumulative_size = 0;
	for (index_t i = 0; i < types.size(); i++) {
		accumulative_tuple_size.push_back(accumulative_size);
		accumulative_size += GetTypeIdSize(types[i]);
	}
	tuple_size = accumulative_size;

	// set up the segment trees for the column segments
	columns = unique_ptr<SegmentTree[]>(new SegmentTree[types.size()]);

	// initialize the table with the existing data from disk
	index_t current_row = InitializeTable(move(data));

	// now initialize the transient segments and the transient version chunk
	for (index_t i = 0; i < types.size(); i++) {
		columns[i].AppendSegment(make_unique<TransientSegment>(types[i], current_row));
	}
	// initialize the table with an empty storage chunk
	AppendVersionChunk(current_row);
}

index_t DataTable::InitializeTable(unique_ptr<vector<unique_ptr<PersistentSegment>>[]> data) {
	if (!data || data[0].size() == 0) {
		// no data: nothing to set up
		return 0;
	}

	index_t current_row = 0;

	// first append all the segments to the set of column segments
	for (index_t i = 0; i < types.size(); i++) {
		for (auto &segment : data[i]) {
			columns[i].AppendSegment(move(segment));
		}
	}

	// set up the initial segments
	auto segments = unique_ptr<ColumnPointer[]>(new ColumnPointer[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		segments[i].segment = (ColumnSegment *)columns[i].GetRootSegment();
		segments[i].offset = 0;
	}

	// now create the version chunks
	while (segments[0].segment) {
		auto chunk = make_unique<VersionChunk>(VersionChunkType::PERSISTENT, *this, current_row);
		// set the columns of the chunk
		chunk->columns = unique_ptr<ColumnPointer[]>(new ColumnPointer[types.size()]);
		for (index_t i = 0; i < types.size(); i++) {
			chunk->columns[i].segment = segments[i].segment;
			chunk->columns[i].offset = segments[i].offset;
		}
		// now advance each of the segments until either (1) the max for the version chunk is reached or (2) the end of
		// the table is reached
		for (index_t i = 0; i < types.size(); i++) {
			index_t count = 0;
			while (count < STORAGE_CHUNK_SIZE) {
				index_t entries_in_segment =
				    std::min(STORAGE_CHUNK_SIZE - count, segments[i].segment->count - segments[i].offset);
				segments[i].offset += entries_in_segment;
				count += entries_in_segment;
				if (segments[i].offset == segments[i].segment->count) {
					// move to the next segment
					if (!segments[i].segment->next) {
						// ran out of segments
						segments[i].segment = nullptr;
						break;
					}
					segments[i].segment = (ColumnSegment *)segments[i].segment->next.get();
					segments[i].offset = 0;
				}
			}
			assert(i == 0 || chunk->count == count);
			chunk->count = count;
		}

		current_row += chunk->count;
		storage_tree.AppendSegment(move(chunk));
	}
	return current_row;
}

VersionChunk *DataTable::AppendVersionChunk(index_t start) {
	auto chunk = make_unique<VersionChunk>(VersionChunkType::TRANSIENT, *this, start);
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

VersionChunk *DataTable::GetChunk(index_t row_number) {
	return (VersionChunk *)storage_tree.GetSegment(row_number);
}

void DataTable::AppendVector(index_t column_index, Vector &data, index_t offset, index_t count) {
	// get the segment to append to
	auto segment = (TransientSegment *)columns[column_index].GetLastSegment();
	// the last segment of a table should always be a transient segment
	assert(segment->segment_type == ColumnSegmentType::TRANSIENT);

	// append the data from the vector
	index_t copied_elements = segment->Append(data, offset, count);
	if (copied_elements < count) {
		// we couldn't fit everything we wanted in the original column segment
		// create a new one
		auto column_segment = make_unique<TransientSegment>(segment->type, segment->start + segment->count);
		columns[column_index].AppendSegment(move(column_segment));
		// now try again
		AppendVector(column_index, data, offset + copied_elements, count - copied_elements);
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
		auto last_chunk = (VersionChunk *)storage_tree.nodes.back().node;
		auto lock = last_chunk->lock.GetExclusiveLock();
		assert(!last_chunk->next);

		// get the start row_id of the chunk
		row_start = last_chunk->start + last_chunk->count;

		// Append the entries to the indexes, we do this first because this might fail in case of unique index conflicts
		AppendToIndexes(chunk, row_start);

		Transaction &transaction = context.ActiveTransaction();
		index_t remainder = chunk.size();
		index_t offset = 0;
		while (remainder > 0) {
			index_t to_copy = min(STORAGE_CHUNK_SIZE - last_chunk->count, remainder);
			if (to_copy > 0) {
				// push deleted entries into the undo buffer
				last_chunk->PushDeletedEntries(transaction, to_copy);
				// now insert the elements into the column segments
				for (index_t i = 0; i < chunk.column_count; i++) {
					AppendVector(i, chunk.data[i], offset, to_copy);
				}
				// now increase the count of the chunk
				last_chunk->count += to_copy;
				offset += to_copy;
				remainder -= to_copy;
			}
			if (remainder > 0) {
				last_chunk = AppendVersionChunk(last_chunk->start + last_chunk->count);
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
		auto version = chunk->GetVersionInfo(id);
		if (VersionInfo::HasConflict(version, transaction.transaction_id)) {
			throw TransactionException("Conflict on tuple deletion!");
		}
		// no conflict, move the current tuple data into the undo buffer
		chunk->PushTuple(transaction, UndoFlags::DELETE_TUPLE, id);
		// and set the deleted flag
		chunk->SetDeleted(id);
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

	// now perform the actual update
	Transaction &transaction = context.ActiveTransaction();

	auto ids = (row_t *)row_identifiers.data;
	auto sel_vector = row_identifiers.sel_vector;
	auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];
	// first find the chunk the row ids belong to
	auto chunk = GetChunk(first_id);

	if (chunk->type == VersionChunkType::PERSISTENT) {
		// persistent chunk, we can't do an in-place update here
		// first fetch the existing columns for any non-updated columns
		updates.Flatten();
		row_identifiers.Flatten();

		unordered_map<column_t, column_t> update_ids;
		vector<column_t> fetch_ids;
		vector<TypeId> fetch_types;
		for (index_t i = 0; i < types.size(); i++) {
			auto entry = std::find(column_ids.begin(), column_ids.end(), i);
			if (entry == column_ids.end()) {
				// column is not present in update list: fetch from base table
				fetch_ids.push_back(i);
				fetch_types.push_back(types[i]);
			} else {
				// column is present in update list: get value from update
				update_ids[i] = entry - column_ids.begin();
			}
		}
		DataChunk append_chunk, fetched_chunk;
		append_chunk.Initialize(types);
		if (fetch_ids.size() > 0) {
			// need to fetch entries from the base table
			fetched_chunk.Initialize(fetch_types);
			Fetch(context.ActiveTransaction(), fetched_chunk, fetch_ids, row_identifiers);
		}

		// now create the append chunk
		for (index_t i = 0; i < types.size(); i++) {
			auto entry = update_ids.find(i);
			if (entry != update_ids.end()) {
				// fetch vector from updates
				append_chunk.data[i].Reference(updates.data[entry->second]);
			} else {
				// fetch vector from fetched chunk
				auto entry = std::find(fetch_ids.begin(), fetch_ids.end(), i);
				append_chunk.data[i].Reference(fetched_chunk.data[entry - fetch_ids.begin()]);
			}
		}

		// append the new set of rows
		Append(table, context, append_chunk);

		// finally delete the current set of rows
		Delete(table, context, row_identifiers);
		return;
	}

	// move strings to a temporary heap
	StringHeap heap;
	updates.MoveStringsToHeap(heap);

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
		auto version = chunk->GetVersionInfo(id);
		if (VersionInfo::HasConflict(version, transaction.transaction_id)) {
			throw TransactionException("Conflict on tuple update!");
		}
	});

	// now we update any indexes, we do this before inserting anything into the undo buffer
	UpdateIndexes(table, column_ids, updates, row_identifiers);

	// now we know there are no conflicts, move the tuples into the undo buffer and mark the chunk as dirty
	VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
		auto id = ids[i] - chunk->start;
		// move the current tuple data into the undo buffer
		chunk->PushTuple(transaction, UndoFlags::UPDATE_TUPLE, id);
	});

	// now update the columns in the base table
	for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto column_id = column_ids[col_idx];

		chunk->Update(row_identifiers, updates.data[col_idx], column_id);
	}
	// after a successful update move the strings into the chunk
	chunk->string_heap.MergeHeap(heap);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state) {
	state.chunk = (VersionChunk *)storage_tree.GetRootSegment();
	state.last_chunk = (VersionChunk *)storage_tree.GetLastSegment();
	state.last_chunk_count = state.last_chunk->count;
	state.columns = unique_ptr<ColumnPointer[]>(new ColumnPointer[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		state.columns[i].segment = (ColumnSegment *)columns[i].GetRootSegment();
		state.columns[i].offset = 0;
	}
	state.offset = 0;
	state.version_chain = nullptr;
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
                     TableScanState &state) {
	// scan the base table
	while (state.chunk) {
		auto current_chunk = state.chunk;

		// scan the current chunk
		bool is_last_segment = current_chunk->Scan(state, transaction, result, column_ids, state.offset);

		if (is_last_segment) {
			// last segment of this chunk: move to next segment
			if (state.chunk == state.last_chunk) {
				state.chunk = nullptr;
				break;
			} else {
				state.offset = 0;
				state.chunk = (VersionChunk *)current_chunk->next.get();
			}
		} else {
			// move to next segment in this chunk
			state.offset++;
		}
		if (result.size() > 0) {
			return;
		}
	}
}

void DataTable::Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids,
                      Vector &row_identifiers) {
	assert(row_identifiers.type == ROW_TYPE);
	auto row_ids = (row_t *)row_identifiers.data;

	VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
		auto row_id = row_ids[i];
		auto chunk = GetChunk(row_id);
		auto lock = chunk->lock.GetSharedLock();

		assert((index_t)row_id >= chunk->start && (index_t)row_id < chunk->start + chunk->count);
		auto index = row_id - chunk->start;

		chunk->RetrieveTupleData(transaction, result, column_ids, index);
	});
}

void DataTable::InitializeIndexScan(IndexTableScanState &state) {
	InitializeScan(state);
	state.version_index = 0;
	state.version_offset = 0;
}

void DataTable::CreateIndexScan(IndexTableScanState &state, vector<column_t> &column_ids, DataChunk &result) {
	while (state.chunk) {
		auto current_chunk = state.chunk;

		bool chunk_exhausted = current_chunk->CreateIndexScan(state, column_ids, result);

		if (chunk_exhausted) {
			// exceeded this chunk, move to next one
			state.chunk = (VersionChunk *)state.chunk->next.get();
			state.offset = 0;
			state.version_index = 0;
			state.version_offset = 0;
			state.version_chain = nullptr;
		}
		if (result.size() > 0) {
			return;
		}
	}
}

void DataTable::RetrieveVersionedData(DataChunk &result, data_ptr_t alternate_version_pointers[],
                                      index_t alternate_version_count) {
	assert(alternate_version_count > 0);
	Vector version_pointers(TypeId::POINTER, (data_ptr_t)alternate_version_pointers);
	version_pointers.count = alternate_version_count;
	// get data from the alternate versions for each column
	for (index_t j = 0; j < result.column_count; j++) {
		VectorOperations::Gather::Append(version_pointers, result.data[j], accumulative_tuple_size[j]);
	}
}

void DataTable::RetrieveVersionedData(DataChunk &result, const vector<column_t> &column_ids,
                                      data_ptr_t alternate_version_pointers[], index_t alternate_version_index[],
                                      index_t alternate_version_count) {
	assert(alternate_version_count > 0);
	// create a vector of the version pointers
	Vector version_pointers(TypeId::POINTER, (data_ptr_t)alternate_version_pointers);
	version_pointers.count = alternate_version_count;
	// get data from the alternate versions for each column
	for (index_t j = 0; j < column_ids.size(); j++) {
		if (column_ids[j] == COLUMN_IDENTIFIER_ROW_ID) {
			assert(result.data[j].type == ROW_TYPE);
			// assign the row identifiers
			auto data = ((row_t *)result.data[j].data) + result.data[j].count;
			for (index_t k = 0; k < alternate_version_count; k++) {
				data[k] = alternate_version_index[k];
			}
			result.data[j].count += alternate_version_count;
		} else {
			// grab data from the stored tuple for each column
			index_t offset = accumulative_tuple_size[column_ids[j]];
			VectorOperations::Gather::Append(version_pointers, result.data[j], offset);
		}
	}
}

void DataTable::AddIndex(unique_ptr<Index> index, vector<unique_ptr<Expression>> &expressions) {
	// initialize an index scan
	IndexTableScanState state;
	InitializeIndexScan(state);

	DataChunk result;
	result.Initialize(index->types);

	DataChunk intermediate;
	vector<TypeId> intermediate_types;
	auto column_ids = index->column_ids;
	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	for (auto &id : index->column_ids) {
		intermediate_types.push_back(types[id]);
	}
	intermediate_types.push_back(ROW_TYPE);
	intermediate.Initialize(intermediate_types);

	// now start incrementally building the index
	while (true) {
		intermediate.Reset();
		// scan a new chunk from the table to index
		CreateIndexScan(state, column_ids, intermediate);
		if (intermediate.size() == 0) {
			// finished scanning for index creation
			// release all locks
			break;
		}
		// resolve the expressions for this chunk
		ExpressionExecutor executor(intermediate);
		executor.Execute(expressions, result);
		// insert into the index
		index->Insert(result, intermediate.data[intermediate.column_count - 1]);
	}
	indexes.push_back(move(index));
}
