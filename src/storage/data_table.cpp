#include "storage/data_table.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
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

StorageChunk* DataTable::AppendStorageChunk(index_t start) {
	auto chunk = make_unique<StorageChunk>(*this, start);
	auto chunk_pointer = chunk.get();
	// set the columns of the chunk
	chunk->columns = unique_ptr<ColumnPointer[]>(new ColumnPointer[types.size()]);
	for(index_t i = 0; i < types.size(); i++) {
		chunk->columns[i].segment = (ColumnSegment*) columns[i].nodes.back().node;
		chunk->columns[i].offset = chunk->columns[i].segment->count;
	}
	storage_tree.AppendSegment(move(chunk));
	return chunk_pointer;
}

void DataTable::InitializeScan(ScanState &state) {
	state.chunk = (StorageChunk*) storage_tree.GetRootSegment();
	state.columns = unique_ptr<ColumnPointer[]>(new ColumnPointer[types.size()]);
	for (index_t i = 0; i < types.size(); i++) {
		state.columns[i].segment = (ColumnSegment*) columns[i].GetRootSegment();
		state.columns[i].offset = 0;
	}
	state.offset = 0;
	state.version_chain = nullptr;
}

StorageChunk *DataTable::GetChunk(index_t row_number) {
	return (StorageChunk*) storage_tree.GetSegment(row_number);
}

void DataTable::VerifyConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : table.bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			if (VectorOperations::HasNull(chunk.data[not_null.index])) {
				throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name.c_str(),
				                          table.columns[not_null.index].name.c_str());
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			ExpressionExecutor executor(chunk);

			Vector result(TypeId::INTEGER, true, false);
			try {
				executor.ExecuteExpression(*check.expression, result);
			} catch (Exception &ex) {
				throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name.c_str(),
				                          ex.GetMessage().c_str());
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
			break;
		}
		case ConstraintType::FOREIGN_KEY:
		case ConstraintType::UNIQUE:
			// we check these constraint later
			// as these checks rely on the data currently stored in the table
			// instead of just on the to-be-inserted chunk
			break;
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

void DataTable::AppendVector(index_t column_index, Vector &data, index_t offset, index_t count) {
	// get the segment to append to
	auto segment = (ColumnSegment*) columns[column_index].GetLastSegment();
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

void DataTable::Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	if (chunk.column_count != table.columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}

	chunk.Verify();
	VerifyConstraints(table, context, chunk);

	// ready to append: obtain an exclusive lock on the last segment
	lock_guard<mutex> tree_lock(storage_tree.node_lock);
	auto last_chunk = (StorageChunk*) storage_tree.nodes.back().node;
	auto lock = last_chunk->lock.GetExclusiveLock();
	assert(!last_chunk->next);

	StringHeap heap;
	chunk.MoveStringsToHeap(heap);

	// we have an exclusive lock on the last chunk
	// now we can append the elements

	// first we handle any PRIMARY KEY and UNIQUE constraints
	UniqueIndex::Append(context.ActiveTransaction(), unique_indexes, chunk, last_chunk->start + last_chunk->count);

	// update the statistics with the new data
	for (index_t i = 0; i < types.size(); i++) {
		statistics[i].Update(chunk.data[i]);
	}

	// now we know no constraints are violated
	// first append the entries to the index
	for (auto &index : indexes) {
		index->Append(context, chunk, last_chunk->start + last_chunk->count);
	}

	Transaction &transaction = context.ActiveTransaction();

	// first copy as much as can fit into the current chunk
	index_t current_count = min(STORAGE_CHUNK_SIZE - last_chunk->count, chunk.size());
	if (current_count > 0) {
		// in the undo buffer, create entries with the "deleted" flag for each
		// tuple so other transactions see the deleted entries before these
		// changes are committed
		transaction.PushDeletedEntries(last_chunk->count, current_count, last_chunk,
		                               last_chunk->version_pointers + last_chunk->count);
		// now insert the elements into the column segments
		for (index_t i = 0; i < chunk.column_count; i++) {
			AppendVector(i, chunk.data[i], 0, current_count);
		}
		// now increase the count of the chunk
		last_chunk->count += current_count;
	}

	// check if we need to append more entries
	if (current_count != chunk.size()) {
		// we need to append more entries
		auto new_chunk = AppendStorageChunk(last_chunk->start + last_chunk->count);

		// now append the remainder
		index_t remainder = chunk.size() - current_count;
		// first push the deleted entries
		transaction.PushDeletedEntries(0, remainder, new_chunk, new_chunk->version_pointers);
		// now append the remaining elements into the column segments
		for (index_t i = 0; i < chunk.column_count; i++) {
			AppendVector(i, chunk.data[i], current_count, remainder);
		}
		new_chunk->count = remainder;
	}
	// after a successful append move the strings to the chunk
	last_chunk->string_heap.MergeHeap(heap);
}

void DataTable::Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers) {
	assert(row_identifiers.type == TypeId::BIGINT);
	if (row_identifiers.count == 0) {
		return;
	}

	Transaction &transaction = context.ActiveTransaction();

	auto ids = (int64_t *)row_identifiers.data;
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
		assert(id >= 0 && id < chunk->count);
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
		// and set the deleted flag
		chunk->deleted[id] = true;
	});
}

void DataTable::Update(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers,
                       vector<column_t> &column_ids, DataChunk &updates) {
	assert(row_identifiers.type == TypeId::BIGINT);
	updates.Verify();
	if (row_identifiers.count == 0) {
		return;
	}

	VerifyConstraints(table, context, updates);

	// move strings to a temporary heap
	StringHeap heap;
	updates.MoveStringsToHeap(heap);

	Transaction &transaction = context.ActiveTransaction();

	auto ids = (int64_t *)row_identifiers.data;
	auto sel_vector = row_identifiers.sel_vector;
	auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];
	// first find the chunk the row ids belong to
	auto chunk = GetChunk(first_id);

	// get an exclusive lock on the chunk
	auto lock = chunk->lock.GetExclusiveLock();

	// first we handle any PRIMARY KEY and UNIQUE constraints
	UniqueIndex::Update(context.ActiveTransaction(), chunk, unique_indexes, column_ids, updates, row_identifiers);

	// no constraints are violated
	// first update any indexes
	for (auto &index : indexes) {
		index->Update(context, column_ids, updates, row_identifiers);
	}

	// now update the entries
	VectorOperations::Exec(row_identifiers, [&](index_t i, index_t k) {
		auto id = ids[i] - chunk->start;
		// assert that all ids in the vector belong to the same chunk
		assert(id >= 0 && id < chunk->count);
		// check for conflicts
		auto version = chunk->version_pointers[id];
		if (version) {
			if (version->version_number >= TRANSACTION_ID_START &&
			    version->version_number != transaction.transaction_id) {
				throw TransactionException("Conflict on tuple update!");
			}
		}
		// no conflict, move the current tuple data into the undo buffer
		transaction.PushTuple(UndoFlags::UPDATE_TUPLE, id, chunk);
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

void DataTable::RetrieveBaseTableData(DataChunk &result, const vector<column_t> &column_ids, sel_t regular_entries[],
                                      index_t regular_count, StorageChunk *current_chunk, index_t current_offset) {
	throw NotImplementedException("No");
}

void DataTable::RetrieveColumnData(Vector &result, TypeId type, ColumnPointer &pointer, index_t count) {
	index_t type_size = GetTypeIdSize(type);
	// copy data from the column storage
	while(count > 0) {
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
			pointer.segment = (ColumnSegment*) pointer.segment->next.get();
			pointer.offset = 0;
		}
	}
}

void DataTable::RetrieveColumnData(Vector &result, TypeId type, ColumnPointer &pointer, index_t count, sel_t *sel_vector, index_t sel_count) {
	sel_t temp_sel[2][STANDARD_VECTOR_SIZE];
	index_t temp_sel_count[2];

	index_t type_size = GetTypeIdSize(type);
	// copy data from the column storage
	while(count > 0) {
		// check how much we can copy from this column segment
		index_t to_copy = std::min(count, (BLOCK_SIZE - pointer.offset) / type_size);
		if (to_copy == 0) {
			// we can't copy anything from this segment, move to the next segment
			assert(pointer.segment->next);
			pointer.segment = (ColumnSegment*) pointer.segment->next.get();
			pointer.offset = 0;
		} else if (to_copy != count) {
			// we can't copy everything from this column segment, but we can copy something
			// first figure out which elements we need to copy from here, and which elements we need to copy from the next column segment
			temp_sel_count[0] = temp_sel_count[1] = 0;
			for(index_t i = 0; i < sel_count; i++) {
				if (sel_vector[i] < to_copy) {
					// belongs to first sel vector
					temp_sel[0][temp_sel_count[0]++] = sel_vector[i];
				} else {
					// belongs to second sel vector
					temp_sel[1][temp_sel_count[1]++] = sel_vector[i];
				}
			}
			// now perform the actual copy from this node with whatever nodes we have
			if (temp_sel_count[0] > 0) {
				data_ptr_t dataptr = pointer.segment->GetData() + pointer.offset;
				Vector source(type, dataptr);
				source.count = temp_sel_count[0];
				source.sel_vector = temp_sel[0];
				// FIXME: use ::Copy instead of ::AppendFromStorage if there are no null values in this segment
				VectorOperations::AppendFromStorage(source, result);
			}
			// now move to the next chunk
			sel_vector = temp_sel[1];
			sel_count = temp_sel_count[1];
			count -= to_copy;
			// move the pointer to the next segment
			assert(pointer.segment->next);
			pointer.segment = (ColumnSegment*) pointer.segment->next.get();
			pointer.offset = 0;
		} else {
			assert(to_copy > 0);
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
		index_t end = min((index_t)STANDARD_VECTOR_SIZE, current_chunk->count - state.offset);
		{
			// start scanning the chunk to check for deleted and version pointers
			for (index_t i = 0; i < end; i++) {
				version_entries[version_count] = regular_entries[regular_count] = i;
				bool has_version = current_chunk->version_pointers[state.offset + i];
				bool is_deleted = current_chunk->deleted[state.offset + i];
				version_count += has_version;
				regular_count += !(is_deleted || has_version);
			}
			// FIXME if regular_count == end, set dirty flag of this segment of the StorageChunk to false
		}

		if (regular_count < end) {
			// there are versions!
			// chase the version pointers
			data_ptr_t alternate_version_pointers[STANDARD_VECTOR_SIZE];
			index_t alternate_version_index[STANDARD_VECTOR_SIZE];
			index_t alternate_version_count = 0;

			for (index_t i = 0; i < version_count; i++) {
				auto version = current_chunk->version_pointers[state.offset + version_entries[i]];
				if (!version || (version->version_number == transaction.transaction_id ||
								version->version_number < transaction.start_time)) {
					// either (1) there is no version anymore as it was cleaned up,
					// or (2) the base table data belongs to the current transaction
					// in this case use the data in the original table
					if (!current_chunk->deleted[state.offset + version_entries[i]]) {
						regular_entries[regular_count++] = version_entries[i];
					}
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
					if (!version->tuple_data) {
						continue;
					} else {
						alternate_version_pointers[alternate_version_count] = version->tuple_data;
						alternate_version_index[alternate_version_count] = state.offset + version_entries[i];
						alternate_version_count++;
					}
				}
			}
			if (alternate_version_count > 0) {
				// retrieve alternate versions, if any
				RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index, alternate_version_count);
			}
			if (regular_count > 0) {
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
						RetrieveColumnData(result.data[col_idx], types[column_ids[col_idx]], state.columns[column_ids[col_idx]], end, regular_entries, regular_count);
					}
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
					RetrieveColumnData(result.data[col_idx], types[column_ids[col_idx]], state.columns[column_ids[col_idx]], regular_count);
				}
			}
		}
		// release the read lock
		state.offset += STANDARD_VECTOR_SIZE;
		if (state.offset >= current_chunk->count) {
			state.offset = 0;
			state.chunk = (StorageChunk*) current_chunk->next.get();
		}
		if (result.size() > 0) {
			return;
		}
	}
}

void DataTable::Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids,
                      Vector &row_identifiers) {
	assert(row_identifiers.type == TypeId::BIGINT);
	auto row_ids = (int64_t *)row_identifiers.data;
	// sort the row identifiers first
	// this is done so we can minimize the amount of chunks that we lock
	sel_t sort_vector[STANDARD_VECTOR_SIZE];
	VectorOperations::Sort(row_identifiers, sort_vector);

	StorageChunk *current_chunk = nullptr;
	unique_ptr<StorageLockKey> lock;
	for (index_t i = 0; i < row_identifiers.count; i++) {
		auto row_id = row_ids[sort_vector[i]];
		auto chunk = GetChunk(row_id);
		if (chunk != current_chunk) {
			// chunk is not locked yet
			// get the lock on the current chunk
			lock = chunk->lock.GetSharedLock();
			current_chunk = chunk;
		}
		assert((index_t)row_id >= chunk->start && (index_t)row_id < chunk->start + chunk->count);
		auto index = row_id - chunk->start;
		bool retrieve_base_version = true;

		// check if this tuple is versioned
		auto version = chunk->version_pointers[index];
		if (version) {
			// tuple is versioned
			// check which tuple to retrieve
			if (version->version_number == transaction.transaction_id ||
			    version->version_number < transaction.start_time) {
				// use the data in the original table
				retrieve_base_version = true;
			} else {
				retrieve_base_version = false;

				data_ptr_t alternate_version_pointer;
				index_t alternate_version_index;

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
				if (!version->tuple_data) {
					// we have to use this version, but it was deleted
					continue;
				} else {
					alternate_version_pointer = version->tuple_data;
					alternate_version_index = row_id;

					RetrieveVersionedData(result, column_ids, &alternate_version_pointer, &alternate_version_index, 1);
				}
			}
		}

		if (retrieve_base_version) {
			if (!chunk->deleted[index]) {
				sel_t regular_entry = index;
				// not versioned, just retrieve the base info
				RetrieveBaseTableData(result, column_ids, &regular_entry, 1, chunk);
			}
		}
	}
}

void DataTable::CreateIndexScan(ScanState &structure, vector<column_t> &column_ids, DataChunk &result) {
	// scan the base table
	index_t result_count = 0;
	while (structure.chunk) {
		auto current_chunk = structure.chunk;
		if (structure.offset == 0 && !structure.version_chain) {
			// first obtain a shared lock on the current chunk, if we don't have
			// it already
			structure.locks.push_back(current_chunk->lock.GetSharedLock());
		}

		data_ptr_t alternate_version_pointers[STANDARD_VECTOR_SIZE];
		index_t alternate_version_index[STANDARD_VECTOR_SIZE];
		index_t alternate_version_count = 0;

		sel_t regular_entries[STANDARD_VECTOR_SIZE];
		index_t regular_count = 0;
		// now start filling the result until we exhaust the tuples
		while (structure.offset < current_chunk->count) {
			auto index = structure.offset;

			if (!structure.version_chain) {
				// first extract the current information, if not deleted
				structure.version_chain = current_chunk->version_pointers[index];
				if (!current_chunk->deleted[index]) {
					regular_entries[regular_count++] = index;
					result_count++;
					if (result_count == STANDARD_VECTOR_SIZE) {
						if (!structure.version_chain) {
							structure.offset++;
						}
						break;
					}
				}
			}

			// now chase the version pointer, if any
			while (structure.version_chain) {
				if (structure.version_chain->tuple_data) {
					alternate_version_pointers[alternate_version_count] = structure.version_chain->tuple_data;
					alternate_version_index[alternate_version_count] = structure.offset;
					alternate_version_count++;
					result_count++;
				}
				structure.version_chain = structure.version_chain->next;
				if (result_count == STANDARD_VECTOR_SIZE) {
					break;
				}
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				break;
			}

			structure.offset++;
			structure.version_chain = nullptr;
		}
		if (structure.offset >= current_chunk->count) {
			// exceeded this chunk, move to next one
			structure.chunk = (StorageChunk*) structure.chunk->next.get();
			structure.offset = 0;
			structure.version_chain = nullptr;
		}
		if (result_count > 0) {
			if (alternate_version_count > 0) {
				// first retrieve the versioned data
				RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index,
									alternate_version_count);
			}
			if (regular_count > 0) {
				// now retrieve the base column data
				RetrieveBaseTableData(result, column_ids, regular_entries, regular_count, current_chunk);
			}
			return;
		}
	}
}
