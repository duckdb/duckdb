#include "storage/data_table.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "parser/constraints/list.hpp"
#include "planner/constraints/bound_check_constraint.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

DataTable::DataTable(StorageManager &storage, string schema, string table, vector<TypeId> types_)
    : cardinality(0), schema(schema), table(table), types(types_), serializer(types), storage(storage) {
	uint64_t accumulative_size = 0;
	for (uint64_t i = 0; i < types.size(); i++) {
		accumulative_tuple_size.push_back(accumulative_size);
		accumulative_size += GetTypeIdSize(types[i]);
	}
	tuple_size = accumulative_size;
	// create empty statistics for the table
	statistics = unique_ptr<ColumnStatistics[]>(new ColumnStatistics[types.size()]);
	// initialize the table with an empty data chunk
	chunk_list = make_unique<StorageChunk>(*this, 0);
	tail_chunk = chunk_list.get();
}

void DataTable::InitializeScan(ScanStructure &structure) {
	structure.chunk = chunk_list.get();
	structure.offset = 0;
	structure.version_chain = nullptr;
}

StorageChunk *DataTable::GetChunk(uint64_t row_number) {
	// FIXME: this could be O(1) in amount of chunks
	// the loop is not necessary because every chunk besides the last
	// has exactly STORAGE_CHUNK_SIZE elements
	StorageChunk *chunk = chunk_list.get();
	while (chunk) {
		if (row_number >= chunk->start && row_number < chunk->start + chunk->count) {
			return chunk;
		}
		chunk = chunk->next.get();
	}
	throw OutOfRangeException("Row identifiers out of bounds!");
}

void DataTable::VerifyConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : table.constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = *reinterpret_cast<NotNullConstraint *>(constraint.get());
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
			for (uint64_t i = 0; i < result.count; i++) {
				uint64_t index = result.sel_vector ? result.sel_vector[i] : i;
				if (!result.nullmask[index] && dataptr[index] == 0) {
					throw ConstraintException("CHECK constraint failed: %s", table.name.c_str());
				}
			}
			break;
		}
		case ConstraintType::DUMMY:
			break;
		case ConstraintType::PRIMARY_KEY:
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

void DataTable::Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	if (chunk.column_count != table.columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}

	chunk.Verify();
	VerifyConstraints(table, context, chunk);

	auto last_chunk = tail_chunk;
	auto lock = last_chunk->GetExclusiveLock();
	while (last_chunk != tail_chunk) {
		// new chunk was added, have to obtain lock of last chunk
		last_chunk = tail_chunk;
		lock = last_chunk->GetExclusiveLock();
	}
	assert(!last_chunk->next);

	StringHeap heap;
	chunk.MoveStringsToHeap(heap);

	// we have an exclusive lock on the last chunk
	// now we can append the elements

	// first we handle any PRIMARY KEY and UNIQUE constraints
	UniqueIndex::Append(context.ActiveTransaction(), unique_indexes, chunk, last_chunk->start + last_chunk->count);

	// update the statistics with the new data
	for (uint64_t i = 0; i < types.size(); i++) {
		statistics[i].Update(chunk.data[i]);
	}

	// now we know no constraints are violated
	// first append the entries to the index
	for (auto &index : indexes) {
		index->Append(context, chunk, last_chunk->start + last_chunk->count);
	}

	Transaction &transaction = context.ActiveTransaction();

	// first copy as much as can fit into the current chunk
	uint64_t current_count = min(STORAGE_CHUNK_SIZE - last_chunk->count, chunk.size());
	if (current_count > 0) {
		// in the undo buffer, create entries with the "deleted" flag for each
		// tuple so other transactions see the deleted entries before these
		// changes are committed
		transaction.PushDeletedEntries(last_chunk->count, current_count, last_chunk,
		                               last_chunk->version_pointers + last_chunk->count);
		// now insert the elements into the vector
		for (uint64_t i = 0; i < chunk.column_count; i++) {
			char *target =
			    last_chunk->columns[i] + last_chunk->count * GetTypeIdSize(GetInternalType(table.columns[i].type));
			VectorOperations::CopyToStorage(chunk.data[i], target, 0, current_count);
		}
		// now increase the count of the chunk
		last_chunk->count += current_count;
	}

	// check if we need to append more entries
	if (current_count != chunk.size()) {
		// we need to append more entries
		// first create a new chunk and lock it
		auto new_chunk = make_unique<StorageChunk>(*this, last_chunk->start + last_chunk->count);
		auto new_chunk_lock = new_chunk->GetExclusiveLock();
		auto new_chunk_pointer = new_chunk.get();
		assert(!last_chunk->next);
		last_chunk->next = move(new_chunk);
		this->tail_chunk = new_chunk_pointer;

		// now append the remainder
		uint64_t remainder = chunk.size() - current_count;
		// first push the deleted entries
		transaction.PushDeletedEntries(0, remainder, new_chunk_pointer, new_chunk_pointer->version_pointers);
		// now insert the elements into the vector
		for (uint64_t i = 0; i < chunk.column_count; i++) {
			char *target = new_chunk_pointer->columns[i];
			VectorOperations::CopyToStorage(chunk.data[i], target, current_count, remainder);
		}
		new_chunk_pointer->count = remainder;
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
	auto lock = chunk->GetExclusiveLock();

	// now delete the entries
	VectorOperations::Exec(row_identifiers, [&](uint64_t i, uint64_t k) {
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
	auto lock = chunk->GetExclusiveLock();

	// first we handle any PRIMARY KEY and UNIQUE constraints
	UniqueIndex::Update(context.ActiveTransaction(), chunk, unique_indexes, column_ids, updates, row_identifiers);

	// no constraints are violated
	// first update any indexes
	for (auto &index : indexes) {
		index->Update(context, column_ids, updates, row_identifiers);
	}

	// now update the entries
	VectorOperations::Exec(row_identifiers, [&](uint64_t i, uint64_t k) {
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
	for (uint64_t j = 0; j < column_ids.size(); j++) {
		auto column_id = column_ids[j];
		auto size = GetTypeIdSize(updates.data[j].type);
		auto base_data = chunk->columns[column_id];

		Vector *update_vector = &updates.data[j];
		Vector null_vector;
		if (update_vector->nullmask.any()) {
			// has NULL values in the nullmask
			// copy them to a temporary vector
			null_vector.Initialize(update_vector->type, false);
			null_vector.count = update_vector->count;
			VectorOperations::CopyToStorage(*update_vector, null_vector.data);
			update_vector = &null_vector;
		}

		VectorOperations::Exec(row_identifiers, [&](uint64_t i, uint64_t k) {
			auto id = ids[i] - chunk->start;
			auto dataptr = base_data + id * size;
			auto update_index = update_vector->sel_vector ? update_vector->sel_vector[k] : k;
			memcpy(dataptr, update_vector->data + update_index * size, size);
		});

		// update the statistics with the new data
		statistics[column_id].Update(updates.data[j]);
	}
	// after a successful update move the strings into the chunk
	chunk->string_heap.MergeHeap(heap);
}

void DataTable::RetrieveVersionedData(DataChunk &result, const vector<column_t> &column_ids,
                                      uint8_t *alternate_version_pointers[], uint64_t alternate_version_index[],
                                      uint64_t alternate_version_count) {
	if (alternate_version_count == 0) {
		return;
	}
	// get data from the alternate versions for each column
	for (uint64_t j = 0; j < column_ids.size(); j++) {
		if (column_ids[j] == COLUMN_IDENTIFIER_ROW_ID) {
			assert(result.data[j].type == TypeId::BIGINT);
			// assign the row identifiers
			auto data = ((int64_t *)result.data[j].data) + result.data[j].count;
			for (uint64_t k = 0; k < alternate_version_count; k++) {
				data[k] = alternate_version_index[k];
			}
		} else {
			// grab data from the stored tuple for each column
			uint64_t tuple_size = GetTypeIdSize(result.data[j].type);
			auto res_data = result.data[j].data + result.data[j].count * tuple_size;
			uint64_t offset = accumulative_tuple_size[column_ids[j]];
			for (uint64_t k = 0; k < alternate_version_count; k++) {
				auto base_data = alternate_version_pointers[k] + offset;
				memcpy(res_data, base_data, tuple_size);
				res_data += tuple_size;
			}
		}
		result.data[j].count += alternate_version_count;
	}
}

void DataTable::RetrieveBaseTableData(DataChunk &result, const vector<column_t> &column_ids, sel_t regular_entries[],
                                      uint64_t regular_count, StorageChunk *current_chunk, uint64_t current_offset) {
	if (regular_count == 0) {
		return;
	}
	for (uint64_t j = 0; j < column_ids.size(); j++) {
		if (column_ids[j] == COLUMN_IDENTIFIER_ROW_ID) {
			// generate the row identifiers
			// first generate a sequence of identifiers
			assert(result.data[j].type == TypeId::BIGINT);
			auto column_indexes = ((int64_t *)result.data[j].data + result.data[j].count);
			for (uint64_t i = 0; i < regular_count; i++) {
				column_indexes[i] = current_chunk->start + current_offset + regular_entries[i];
			}
			result.data[j].count += regular_count;
		} else {
			// normal column
			// grab the data from the source using a selection vector
			char *dataptr = current_chunk->columns[column_ids[j]] + GetTypeIdSize(result.data[j].type) * current_offset;
			Vector source(result.data[j].type, dataptr);
			source.sel_vector = regular_entries;
			source.count = regular_count;
			// append while converting NullValue<T> to the nullmask
			VectorOperations::AppendFromStorage(source, result.data[j]);
		}
	}
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
                     ScanStructure &structure) {
	// scan the base table
	while (structure.chunk) {
		auto current_chunk = structure.chunk;
		// first obtain a shared lock on the current chunk
		auto lock = current_chunk->GetSharedLock();
		// now scan the chunk until we find enough pieces to fill a vector or
		// reach the end
		sel_t regular_entries[STANDARD_VECTOR_SIZE], version_entries[STANDARD_VECTOR_SIZE];
		uint64_t regular_count = 0, version_count = 0;
		uint64_t end = min((uint64_t)STANDARD_VECTOR_SIZE, current_chunk->count - structure.offset);
		for (uint64_t i = 0; i < end; i++) {
			version_entries[version_count] = regular_entries[regular_count] = i;
			bool has_version = current_chunk->version_pointers[structure.offset + i];
			bool is_deleted = current_chunk->deleted[structure.offset + i];
			version_count += has_version;
			regular_count += !(is_deleted || has_version);
		}

		if (regular_count < end) {
			// first chase the version pointers, if there are any
			uint8_t *alternate_version_pointers[STANDARD_VECTOR_SIZE];
			uint64_t alternate_version_index[STANDARD_VECTOR_SIZE];
			uint64_t alternate_version_count = 0;

			for (uint64_t i = 0; i < version_count; i++) {
				auto version = current_chunk->version_pointers[structure.offset + version_entries[i]];
				if (!version || (version->version_number == transaction.transaction_id ||
				                 version->version_number < transaction.start_time)) {
					// use the data in the original table
					if (!current_chunk->deleted[structure.offset + version_entries[i]]) {
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
						alternate_version_index[alternate_version_count] = structure.offset + version_entries[i];
						alternate_version_count++;
					}
				}
			}
			RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index,
			                      alternate_version_count);
		}
		// copy the regular entries
		RetrieveBaseTableData(result, column_ids, regular_entries, regular_count, current_chunk, structure.offset);
		// release the read lock
		structure.offset += STANDARD_VECTOR_SIZE;
		if (structure.offset >= current_chunk->count) {
			structure.offset = 0;
			structure.chunk = current_chunk->next.get();
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
	unique_ptr<StorageLock> lock;
	for (uint64_t i = 0; i < row_identifiers.count; i++) {
		auto row_id = row_ids[sort_vector[i]];
		auto chunk = GetChunk(row_id);
		if (chunk != current_chunk) {
			// chunk is not locked yet
			// get the lock on the current chunk
			lock = chunk->GetSharedLock();
			current_chunk = chunk;
		}
		assert((uint64_t)row_id >= chunk->start && (uint64_t)row_id < chunk->start + chunk->count);
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

				uint8_t *alternate_version_pointer;
				uint64_t alternate_version_index;

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

void DataTable::CreateIndexScan(ScanStructure &structure, vector<column_t> &column_ids, DataChunk &result) {
	// scan the base table
	uint64_t result_count = 0;
	while (structure.chunk) {
		auto current_chunk = structure.chunk;
		if (structure.offset == 0 && !structure.version_chain) {
			// first obtain a shared lock on the current chunk, if we don't have
			// it already
			structure.locks.push_back(current_chunk->GetSharedLock());
		}

		uint8_t *alternate_version_pointers[STANDARD_VECTOR_SIZE];
		uint64_t alternate_version_index[STANDARD_VECTOR_SIZE];
		uint64_t alternate_version_count = 0;

		sel_t regular_entries[STANDARD_VECTOR_SIZE];
		uint64_t regular_count = 0;
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
			structure.chunk = structure.chunk->next.get();
			structure.offset = 0;
			structure.version_chain = nullptr;
		}
		if (result_count > 0) {
			// first retrieve the versioned data
			RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index,
			                      alternate_version_count);
			// now retrieve the base column data
			RetrieveBaseTableData(result, column_ids, regular_entries, regular_count, current_chunk);
			return;
		}
	}
}
