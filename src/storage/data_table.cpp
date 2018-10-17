
#include "storage/data_table.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/types/vector_operations.hpp"

#include "execution/expression_executor.hpp"

#include "main/client_context.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"

#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

#include "parser/constraints/list.hpp"

using namespace duckdb;
using namespace std;

DataTable::DataTable(StorageManager &storage, TableCatalogEntry &table)
    : table(table), serializer(table.GetTypes(), false), storage(storage) {
	size_t accumulative_size = 0;
	for (size_t i = 0; i < table.columns.size(); i++) {
		accumulative_tuple_size.push_back(accumulative_size);
		accumulative_size += GetTypeIdSize(table.columns[i].type);
	}
	tuple_size = accumulative_size;
	// create empty statistics for the table
	statistics = unique_ptr<Statistics[]>(new Statistics[table.columns.size()]);
	for (size_t i = 0; i < table.columns.size(); i++) {
		statistics[i].type = table.columns[i].type;
	}
	statistics_locks = unique_ptr<mutex[]>(new mutex[table.columns.size()]);
	// initialize the table with an empty data chunk
	chunk_list = make_unique<StorageChunk>(*this, 0);
	tail_chunk = chunk_list.get();
}

vector<TypeId> DataTable::GetTypes(const std::vector<column_t> &column_ids) {
	vector<TypeId> types;
	for (auto &index : column_ids) {
		if (index == COLUMN_IDENTIFIER_ROW_ID) {
			types.push_back(TypeId::POINTER);
		} else {
			types.push_back(table.columns[index].type);
		}
	}
	return types;
}

void DataTable::InitializeScan(ScanStructure &structure) {
	structure.chunk = chunk_list.get();
	structure.offset = 0;
}

StorageChunk *DataTable::GetChunk(size_t row_number) {
	// FIXME: this could be O(1) in amount of chunks
	// the loop is not necessary because every chunk besides the last
	// has exactly STORAGE_CHUNK_SIZE elements
	StorageChunk *chunk = chunk_list.get();
	while (chunk) {
		if (row_number >= chunk->start &&
		    row_number < chunk->start + chunk->count) {
			return chunk;
		}
		chunk = chunk->next.get();
	}
	return nullptr;
}

void DataTable::VerifyConstraints(ClientContext &context, DataChunk &chunk) {
	for (auto &constraint : table.constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null =
			    *reinterpret_cast<NotNullConstraint *>(constraint.get());
			if (VectorOperations::HasNull(chunk.data[not_null.index])) {
				throw ConstraintException(
				    "NOT NULL constraint failed: %s.%s", table.name.c_str(),
				    table.columns[not_null.index].name.c_str());
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check =
			    *reinterpret_cast<CheckConstraint *>(constraint.get());
			ExpressionExecutor executor(chunk, context);

			Vector result(TypeId::INTEGER, true, false);
			try {
				executor.Execute(check.expression.get(), result);
			} catch (Exception &ex) {
				throw ConstraintException(
				    "CHECK constraint failed: %s (Error: %s)",
				    table.name.c_str(), ex.GetMessage().c_str());
			} catch (...) {
				throw ConstraintException(
				    "CHECK constraint failed: %s (Unknown Error)",
				    table.name.c_str());
			}

			int *dataptr = (int *)result.data;
			for (size_t i = 0; i < result.count; i++) {
				size_t index = result.sel_vector ? result.sel_vector[i] : i;
				if (!result.nullmask[index] && dataptr[index] == 0) {
					throw ConstraintException("CHECK constraint failed: %s",
					                          table.name.c_str());
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

void DataTable::Append(ClientContext &context, DataChunk &chunk) {
	if (chunk.count == 0) {
		return;
	}
	if (chunk.column_count != table.columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}

	chunk.Verify();
	VerifyConstraints(context, chunk);

	auto last_chunk = tail_chunk;
	do {
		last_chunk->GetExclusiveLock();
		if (last_chunk != tail_chunk) {
			// new chunk was added, have to obtain lock of last chunk
			last_chunk->ReleaseExclusiveLock();
			last_chunk = tail_chunk;
			continue;
		}
	} while (false);

	// we have an exclusive lock on the last chunk
	// now we can append the elements

	// first we handle any PRIMARY KEY and UNIQUE constraints
	auto error =
	    UniqueIndex::Append(context.ActiveTransaction(), indexes, chunk,
	                        last_chunk->start + last_chunk->count);
	if (!error.empty()) {
		last_chunk->ReleaseExclusiveLock();
		throw ConstraintException(error);
	}

	// update the statistics with the new data
	for (size_t i = 0; i < table.columns.size(); i++) {
		lock_guard<mutex> stats_lock(statistics_locks[i]);
		statistics[i].Update(chunk.data[i]);
	}

	// move any string heaps from the vectors to the storage
	for (size_t i = 0; i < table.columns.size(); i++) {
		last_chunk->string_heap.MergeHeap(chunk.data[i].string_heap);
	}

	Transaction &transaction = context.ActiveTransaction();

	// first copy as much as can fit into the current chunk
	size_t current_count =
	    std::min(STORAGE_CHUNK_SIZE - last_chunk->count, chunk.count);
	if (current_count > 0) {
		// in the undo buffer, create entries with the "deleted" flag for each
		// tuple so other transactions see the deleted entries before these
		// changes are committed
		transaction.PushDeletedEntries(
		    last_chunk->count, current_count, last_chunk,
		    last_chunk->version_pointers + last_chunk->count);
		// now insert the elements into the vector
		for (size_t i = 0; i < chunk.column_count; i++) {
			char *target =
			    last_chunk->columns[i] +
			    last_chunk->count * GetTypeIdSize(table.columns[i].type);
			VectorOperations::CopyNull(chunk.data[i], target, 0, current_count);
		}
		// now increase the count of the chunk
		last_chunk->count += current_count;
	}

	// check if we need to append more entries
	if (current_count != chunk.count) {
		// we need to append more entries
		// first create a new chunk and lock it
		auto new_chunk = make_unique<StorageChunk>(
		    *this, last_chunk->start + last_chunk->count);
		new_chunk->GetExclusiveLock();
		auto new_chunk_pointer = new_chunk.get();
		assert(!last_chunk->next);
		last_chunk->next = move(new_chunk);
		this->tail_chunk = new_chunk_pointer;

		// now append the remainder
		size_t remainder = chunk.count - current_count;
		// first push the deleted entries
		transaction.PushDeletedEntries(0, remainder, new_chunk_pointer,
		                               new_chunk_pointer->version_pointers);
		// now insert the elements into the vector
		for (size_t i = 0; i < chunk.column_count; i++) {
			char *target = new_chunk_pointer->columns[i];
			VectorOperations::CopyNull(chunk.data[i], target, current_count,
			                           remainder);
		}
		new_chunk_pointer->count = remainder;
		new_chunk_pointer->ReleaseExclusiveLock();
	}
	// everything has been appended: release lock
	last_chunk->ReleaseExclusiveLock();
}

void DataTable::Delete(ClientContext &context, Vector &row_identifiers) {
	if (row_identifiers.type != TypeId::POINTER) {
		throw InvalidTypeException(row_identifiers.type,
		                           "Row identifiers must be POINTER type!");
	}
	if (row_identifiers.count == 0) {
		return;
	}

	Transaction &transaction = context.ActiveTransaction();

	auto ids = (uint64_t *)row_identifiers.data;
	auto sel_vector = row_identifiers.sel_vector;
	auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];
	// first find the chunk the row ids belong to
	auto chunk = GetChunk(first_id);
	if (!chunk) {
		throw OutOfRangeException(
		    "Row identifiers for deletion out of bounds!");
	}

	// get an exclusive lock on the chunk
	chunk->GetExclusiveLock();

	// now delete the entries
	for (size_t i = 0; i < row_identifiers.count; i++) {
		auto id = (sel_vector ? ids[sel_vector[i]] : ids[i]) - chunk->start;
		// assert that all ids in the vector belong to the same storage
		// chunk
		assert(id >= chunk->start && id < chunk->start + chunk->count);
		// check for conflicts
		auto version = chunk->version_pointers[id];
		if (version) {
			if (version->version_number >= TRANSACTION_ID_START &&
			    version->version_number != transaction.transaction_id) {
				throw TransactionException("Conflict on tuple deletion!");
			}
		}
		// no conflict, move the current tuple data into the undo buffer
		transaction.PushTuple(id, chunk);
		// and set the deleted flag
		chunk->deleted[id] = true;
	}
	chunk->ReleaseExclusiveLock();
}

void DataTable::Update(ClientContext &context, Vector &row_identifiers,
                       vector<column_t> &column_ids, DataChunk &updates) {
	if (row_identifiers.type != TypeId::POINTER) {
		throw InvalidTypeException(row_identifiers.type,
		                           "Row identifiers must be POINTER type!");
	}
	updates.Verify();
	if (row_identifiers.count == 0) {
		return;
	}

	VerifyConstraints(context, updates);

	Transaction &transaction = context.ActiveTransaction();

	auto ids = (uint64_t *)row_identifiers.data;
	auto sel_vector = row_identifiers.sel_vector;
	auto first_id = sel_vector ? ids[sel_vector[0]] : ids[0];
	// first find the chunk the row ids belong to
	auto chunk = GetChunk(first_id);
	if (!chunk) {
		throw OutOfRangeException("Row identifiers for update out of bounds!");
	}

	// get an exclusive lock on the chunk
	chunk->GetExclusiveLock();

	// first we handle any PRIMARY KEY and UNIQUE constraints
	if (indexes.size() > 0) {
		auto error =
		    UniqueIndex::Update(context.ActiveTransaction(), chunk, indexes,
		                        column_ids, updates, row_identifiers);
		if (!error.empty()) {
			chunk->ReleaseExclusiveLock();
			throw ConstraintException(error);
		}
	}

	// now update the entries
	for (size_t i = 0; i < row_identifiers.count; i++) {
		auto id = (sel_vector ? ids[sel_vector[i]] : ids[i]) - chunk->start;
		// assert that all ids in the vector belong to the same chunk
		assert(id >= chunk->start && id < chunk->start + chunk->count);
		// check for conflicts
		auto version = chunk->version_pointers[id];
		if (version) {
			if (version->version_number >= TRANSACTION_ID_START &&
			    version->version_number != transaction.transaction_id) {
				throw TransactionException("Conflict on tuple update!");
			}
		}
		// no conflict, move the current tuple data into the undo buffer
		transaction.PushTuple(id, chunk);
	}
	// now update the columns in the base table
	for (size_t j = 0; j < column_ids.size(); j++) {
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
			VectorOperations::CopyNull(*update_vector, null_vector.data);
			update_vector = &null_vector;
		}

		if (update_vector->sel_vector) {
			for (size_t i = 0; i < row_identifiers.count; i++) {
				auto id =
				    (sel_vector ? ids[sel_vector[i]] : ids[i]) - chunk->start;
				auto dataptr = base_data + id * size;
				memcpy(dataptr,
				       update_vector->data +
				           update_vector->sel_vector[i] * size,
				       size);
			}
		} else {
			for (size_t i = 0; i < row_identifiers.count; i++) {
				auto id =
				    (sel_vector ? ids[sel_vector[i]] : ids[i]) - chunk->start;
				auto dataptr = base_data + id * size;
				memcpy(dataptr, update_vector->data + i * size, size);
			}
		}
		chunk->string_heap.MergeHeap(update_vector->string_heap);

		// update the statistics with the new data
		lock_guard<mutex> stats_lock(statistics_locks[column_id]);
		statistics[column_id].Update(updates.data[j]);
	}
	chunk->ReleaseExclusiveLock();
}

void DataTable::Scan(Transaction &transaction, DataChunk &result,
                     const vector<column_t> &column_ids,
                     ScanStructure &structure) {
	// scan the base table
	while (structure.chunk) {
		auto current_chunk = structure.chunk;
		// first obtain a shared lock on the current chunk
		current_chunk->GetSharedLock();
		// now scan the chunk until we find enough pieces to fill a vector or
		// reach the end
		sel_t regular_entries[STANDARD_VECTOR_SIZE],
		    version_entries[STANDARD_VECTOR_SIZE];
		size_t regular_count = 0, version_count = 0;
		size_t end = std::min((size_t)STANDARD_VECTOR_SIZE,
		                      current_chunk->count - structure.offset);
		for (size_t i = 0; i < end; i++) {
			version_entries[version_count] = regular_entries[regular_count] = i;
			bool has_version =
			    current_chunk->version_pointers[structure.offset + i];
			bool is_deleted = current_chunk->deleted[structure.offset + i];
			version_count += has_version;
			regular_count += !(is_deleted || has_version);
		}

		result.count = 0;
		if (regular_count < end) {
			// first chase the version pointers, if there are any
			uint8_t *alternate_version_pointers[STANDARD_VECTOR_SIZE];
			size_t alternate_version_index[STANDARD_VECTOR_SIZE];
			size_t alternate_version_count = 0;

			for (size_t i = 0; i < version_count; i++) {
				auto version =
				    current_chunk->version_pointers[structure.offset +
				                                    version_entries[i]];
				if (!version ||
				    (version->version_number == transaction.transaction_id ||
				     version->version_number < transaction.start_time)) {
					// use the data in the original table
					if (!current_chunk
					         ->deleted[structure.offset + version_entries[i]]) {
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
						if (next->version_number ==
						    transaction.transaction_id) {
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
						alternate_version_pointers[alternate_version_count] =
						    version->tuple_data;
						alternate_version_index[alternate_version_count] =
						    structure.offset + version_entries[i];
						alternate_version_count++;
					}
				}
			}
			if (alternate_version_count > 0) {
				// get data from the alternate versions for each column
				for (size_t j = 0; j < column_ids.size(); j++) {
					if (column_ids[j] == COLUMN_IDENTIFIER_ROW_ID) {
						// assign the row identifiers
						uint64_t *data = (uint64_t *)result.data[j].data;
						for (size_t k = 0; k < alternate_version_count; k++) {
							data[k] = alternate_version_index[k];
						}
					} else {
						// grab data from the stored tuple for each column
						size_t tuple_size = GetTypeIdSize(result.data[j].type);
						auto res_data = result.data[j].data;
						size_t offset = accumulative_tuple_size[column_ids[j]];
						for (size_t k = 0; k < alternate_version_count; k++) {
							auto base_data =
							    alternate_version_pointers[k] + offset;
							memcpy(res_data, base_data, tuple_size);
							res_data += tuple_size;
						}
					}
					result.data[j].count += alternate_version_count;
				}
				result.count += alternate_version_count;
			}
		}
		// copy the regular entries
		if (regular_count > 0) {
			for (size_t j = 0; j < column_ids.size(); j++) {
				if (column_ids[j] == COLUMN_IDENTIFIER_ROW_ID) {
					// generate the row identifiers
					// first generate a sequence of identifiers
					Vector source(TypeId::POINTER, true, false);
					source.count = STANDARD_VECTOR_SIZE;
					VectorOperations::GenerateSequence(
					    source, current_chunk->start + structure.offset);
					// then apply the selection vector
					source.sel_vector = regular_entries;
					source.count = regular_count;
					// append while converting NullValue<T> to the nullmask
					VectorOperations::AppendNull(source, result.data[j]);
				} else {
					// normal column
					// grab the data from the source using a selection vector
					char *dataptr =
					    current_chunk->columns[column_ids[j]] +
					    GetTypeIdSize(result.data[j].type) * structure.offset;
					Vector source(result.data[j].type, dataptr);
					source.sel_vector = regular_entries;
					source.count = regular_count;
					// append while converting NullValue<T> to the nullmask
					VectorOperations::AppendNull(source, result.data[j]);
				}
			}
			result.count += regular_count;
		}
		// release the read lock
		structure.offset += STANDARD_VECTOR_SIZE;
		if (structure.offset >= current_chunk->count) {
			structure.offset = 0;
			structure.chunk = current_chunk->next.get();
		}
		current_chunk->ReleaseSharedLock();
		if (result.count > 0) {
			return;
		}
	}
}
