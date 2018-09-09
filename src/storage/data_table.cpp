
#include "storage/data_table.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/types/vector_operations.hpp"

#include "catalog/table_catalog.hpp"

#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

DataTable::DataTable(StorageManager &storage, TableCatalogEntry &table)
	: storage(storage), count(0), table(table) {
	// create empty statistics for the table
	statistics = unique_ptr<Statistics[]>(new Statistics[table.columns.size()]);
	for(size_t i = 0; i < table.columns.size(); i++) {
		statistics[i].type = table.columns[i].type;
	}
	statistics_locks = unique_ptr<mutex[]>(new mutex[table.columns.size()]);
	// initialize the table with an empty data chunk
	chunk_list = make_unique<StorageChunk>(table.columns);
	tail_chunk = chunk_list.get();
}

vector<TypeId> DataTable::GetTypes(const std::vector<size_t>& column_ids) {
	vector<TypeId> types;
	for(auto &index : column_ids) {
		types.push_back(table.columns[index].type);
	}
	return types;
}

void DataTable::Append(Transaction &transaction, DataChunk &chunk) {
	if (chunk.count == 0) {
		return;
	}
	if (chunk.column_count != table.columns.size()) {
		throw CatalogException("Mismatch in column count for append");
	}

	auto last_chunk = tail_chunk;
	do {
		last_chunk->GetExclusiveLock();
		if (last_chunk != tail_chunk) {
			// new chunk was added, have to obtain lock of last chunk
			last_chunk->ReleaseExclusiveLock();
			last_chunk = tail_chunk;
			continue;
		}
	} while(false);
	
	// we have an exclusive lock on the last chunk
	// now we can append the elements

	// update the statistics with the new data
	for(size_t i = 0; i < table.columns.size(); i++) {
		lock_guard<mutex> stats_lock(statistics_locks[i]);
		statistics[i].Update(chunk.data[i]);
	}

	// move any string heaps from the vectors to the storage
	for(size_t i = 0; i < table.columns.size(); i++) {
		last_chunk->string_heap.MergeHeap(chunk.data[i].string_heap);
	}

	// first copy as much as can fit into the current chunk
	size_t current_count = std::min(STORAGE_CHUNK_SIZE - last_chunk->count, chunk.count);
	if (current_count > 0) {
		// in the undo buffer, create entries with the "deleted" flag for each tuple
		// so other transactions see the deleted entries before these changes are committed
		transaction.PushDeletedEntries(last_chunk->count, current_count, last_chunk, last_chunk->version_pointers + last_chunk->count);
		// now insert the elements into the vector
		for(size_t i = 0; i < chunk.column_count; i++) {
			char *target = last_chunk->columns[i].data + last_chunk->count * GetTypeIdSize(table.columns[i].type);
			VectorOperations::CopyNull(chunk.data[i], target, 0, current_count);
		}
		// now increase the count of the chunk
		last_chunk->count += current_count;
	}

	// check if we need to append more entries
	if (current_count != chunk.count) {
		// we need to append more entries
		// first create a new chunk and lock it
		auto new_chunk = make_unique<StorageChunk>(table.columns);
		new_chunk->GetExclusiveLock();
		auto new_chunk_pointer = new_chunk.get();
		assert(!last_chunk->next);
		last_chunk->next = move(new_chunk);
		this->tail_chunk = new_chunk_pointer;

		// now append the remainder
		size_t remainder = chunk.count - current_count;
		// first push the deleted entries
		transaction.PushDeletedEntries(0, remainder, new_chunk_pointer, new_chunk_pointer->version_pointers);
		// now insert the elements into the vector
		for(size_t i = 0; i < chunk.column_count; i++) {
			char *target = new_chunk_pointer->columns[i].data;
			VectorOperations::CopyNull(chunk.data[i], target, current_count, remainder);
		}
		new_chunk_pointer->count = remainder;
		new_chunk_pointer->ReleaseExclusiveLock();
	}
	// everything has been appended: release lock
	last_chunk->ReleaseExclusiveLock();
}

void DataTable::InitializeScan(ScanStructure &structure) {
	structure.chunk = chunk_list.get();
	structure.offset = 0;
}

void DataTable::Scan(Transaction &transaction, DataChunk &result, const vector<size_t>& column_ids, ScanStructure &structure) {
	// scan the base table
	while (structure.chunk) {
		auto current_chunk = structure.chunk;
		// first obtain a shared lock on the current chunk
		current_chunk->GetSharedLock();
		// now scan the chunk until we find enough pieces to fill a vector or reach the end
		sel_t regular_entries[STANDARD_VECTOR_SIZE], version_entries[STANDARD_VECTOR_SIZE];
		size_t regular_count = 0, version_count = 0;
		size_t end = std::min((size_t) STANDARD_VECTOR_SIZE, current_chunk->count - structure.offset);
		for(size_t i = 0; i < end; i++) {
			version_entries[version_count] = regular_entries[regular_count] = i;
			bool has_version = current_chunk->version_pointers[structure.offset + i].get();
			bool is_deleted = current_chunk->deleted[structure.offset + i];
			version_count += has_version;
			regular_count += !(is_deleted || has_version);
		}

		result.count = 0;
		if (regular_count < end) {
			// first chase the version pointers, if there are any
			Vector alternate_versions(TypeId::POINTER, true);
			void **alternate_version_pointers = (void**) alternate_versions.data;

			for(size_t i = 0; i < version_count; i++) {
				auto version = current_chunk->version_pointers[structure.offset + version_entries[i]];
				if (version->version_number == transaction.transaction_id ||
					version->version_number < transaction.start_time) {
					// use the data in the original table
					regular_entries[regular_count++] = version_entries[i];
				} else {
					// follow the version pointers
					while (true) {
						if (!version->next) {
							// use this version: no predecessor
							break;
						}
						if (version->next->version_number == transaction.transaction_id) {
							// use this version: it was created by us
							break;
						}
						if (version->next->version_number < transaction.start_time) {
							// use this version: it was committed by us
							break;
						}
						version = version->next;
					}
					if (!version->tuple_data) {
						continue;
					} else {
						alternate_version_pointers[alternate_versions.count++] = version->tuple_data;
					}
				}
			}
			if (alternate_versions.count > 0) {
				// FIXME: get data from the alternate versions for each column
				result.count += alternate_versions.count;
				throw NotImplementedException("Alternate versions with data not implemented yet!");
			}
		}
		// copy the regular entries
		if (regular_count > 0) {
			for(size_t j = 0; j < column_ids.size(); j++) {
				char *dataptr = current_chunk->columns[column_ids[j]].data + 
					GetTypeIdSize(result.data[j].type) * structure.offset;
				Vector source(result.data[j].type, dataptr);
				source.sel_vector = regular_entries;
				source.count = regular_count;
				source.CopyNull(result.data[j]);
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

