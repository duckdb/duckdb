#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "storage/data_table.hpp"
#include "transaction/transaction.hpp"
#include "transaction/version_info.hpp"
#include "storage/table/transient_segment.hpp"

using namespace duckdb;
using namespace std;

VersionChunk::VersionChunk(VersionChunkType type, DataTable &base_table, index_t start)
    : SegmentBase(start, 0), type(type), table(base_table) {
}

index_t VersionChunk::GetVersionIndex(index_t index) {
	assert(index <= STORAGE_CHUNK_SIZE);
	return index / STANDARD_VECTOR_SIZE;
}

VersionInfo *VersionChunk::GetVersionInfo(index_t index) {
	index_t version_index = GetVersionIndex(index);
	auto version = version_data[version_index];
	if (!version) {
		return nullptr;
	}
	assert(index >= version->start && index < version->start + STANDARD_VECTOR_SIZE);
	return version->version_pointers[index - version->start];
}

void VersionChunk::SetDeleted(index_t index) {
	// FIXME: assert that StorageLock is held here
	index_t version_index = GetVersionIndex(index);
	auto version = GetOrCreateVersionInfo(version_index);
	version->deleted[index - version->start] = true;
}

VersionChunkInfo *VersionChunk::GetOrCreateVersionInfo(index_t version_index) {
	assert(version_index < STORAGE_CHUNK_VECTORS);
	if (!version_data[version_index]) {
		version_data[version_index] = make_shared<VersionChunkInfo>(*this, version_index * STANDARD_VECTOR_SIZE);
	}
	return version_data[version_index].get();
}

void VersionChunk::PushDeletedEntries(Transaction &transaction, index_t amount) {
	index_t version_index = GetVersionIndex(this->count);
	index_t offset_in_version = this->count % STANDARD_VECTOR_SIZE;

	auto version = GetOrCreateVersionInfo(version_index);
	for (index_t i = 0; i < amount; i++) {
		auto ptr = transaction.PushTuple(UndoFlags::INSERT_TUPLE, 0);
		auto meta = (VersionInfo *)ptr;
		meta->tuple_data = nullptr;
		meta->version_number = transaction.transaction_id;
		meta->entry = offset_in_version;
		meta->vinfo = version;
		meta->prev = nullptr;
		meta->next = nullptr;

		version->version_pointers[offset_in_version] = meta;
		offset_in_version++;
		if (offset_in_version == STANDARD_VECTOR_SIZE) {
			offset_in_version = 0;
			version_index++;
			if (version_index < STORAGE_CHUNK_VECTORS) {
				version = GetOrCreateVersionInfo(version_index);
			} else {
				assert(i + 1 == amount);
			}
		}
	}
}

void VersionChunk::PushTuple(Transaction &transaction, UndoFlags flag, index_t offset) {
	// push the tuple into the undo buffer
	auto ptr = transaction.PushTuple(flag, table.tuple_size);

	auto meta = (VersionInfo *)ptr;
	auto tuple_data = ptr + sizeof(VersionInfo);

	// get the version info meta chunk
	index_t version_index = GetVersionIndex(offset);
	index_t offset_in_version = offset % STANDARD_VECTOR_SIZE;
	auto version = GetOrCreateVersionInfo(version_index);

	// fill in the meta data for the tuple
	meta->tuple_data = tuple_data;
	meta->version_number = transaction.transaction_id;
	meta->entry = offset_in_version;
	meta->vinfo = version;

	meta->prev = nullptr;
	meta->next = version->version_pointers[offset_in_version];
	version->version_pointers[offset_in_version] = meta;

	if (meta->next) {
		meta->next->prev = meta;
	}

	DataChunk chunk;
	chunk.Initialize(table.types);

	for (index_t i = 0; i < table.types.size(); i++) {
		columns[i].segment->Fetch(chunk.data[i], start + offset);
	}

	data_ptr_t target_locations[1];
	target_locations[0] = tuple_data;
	Vector target(TypeId::POINTER, (data_ptr_t)target_locations);
	target.count = 1;
	for (index_t i = 0; i < chunk.column_count; i++) {
		VectorOperations::Scatter::SetAll(chunk.data[i], target);
		target_locations[0] += columns[i].segment->type_size;
	}
}

void VersionChunk::RetrieveTupleFromBaseTable(DataChunk &result, vector<column_t> &column_ids, row_t row_id) {
	assert(result.size() < STANDARD_VECTOR_SIZE);
	assert(column_ids.size() == result.column_count);
	for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
			((row_t *)result.data[col_idx].data)[result.data[col_idx].count] = row_id;
			result.data[col_idx].count++;
		} else {
			assert(column_ids[col_idx] < table.types.size());
			columns[column_ids[col_idx]].segment->Fetch(result.data[col_idx], row_id);
		}
	}
}

void VersionChunk::AppendToChunk(DataChunk &chunk, VersionInfo *info) {
	if (!info->prev) {
		// fetch from base table
		auto id = info->GetRowId();
		for (index_t i = 0; i < table.types.size(); i++) {
			columns[i].segment->Fetch(chunk.data[i], id);
		}
	} else {
		// fetch from tuple data
		assert(info->prev->tuple_data);
		auto tuple_data = info->prev->tuple_data;
		index_t current_offset = chunk.size();
		for (index_t i = 0; i < table.types.size(); i++) {
			auto type = chunk.data[i].type;
			index_t value_size = GetTypeIdSize(type);
			memcpy(chunk.data[i].data + value_size * current_offset, tuple_data, value_size);
			tuple_data += value_size;
			chunk.data[i].count++;
		}
	}
}

void VersionChunk::Update(Vector &row_identifiers, Vector &update_vector, index_t col_idx) {
	// in-place updates can only be done on transient segments
	assert(columns[col_idx].segment->segment_type == ColumnSegmentType::TRANSIENT);
	assert(update_vector.type == columns[col_idx].segment->type);

	auto size = columns[col_idx].segment->type_size;
	auto ids = (row_t *)row_identifiers.data;
	auto &transient = (TransientSegment &)*columns[col_idx].segment;

	if (update_vector.nullmask.any()) {
		// has NULL values in the nullmask
		// copy them to a temporary vector
		Vector null_vector;
		null_vector.Initialize(update_vector.type, false);
		null_vector.count = update_vector.count;
		VectorOperations::CopyToStorage(update_vector, null_vector.data);

		assert(!null_vector.sel_vector);
		VectorOperations::Exec(row_identifiers,
		                       [&](index_t i, index_t k) { transient.Update(ids[i], null_vector.data + k * size); });
	} else {
		assert(row_identifiers.sel_vector == update_vector.sel_vector);
		VectorOperations::Exec(row_identifiers,
		                       [&](index_t i, index_t k) { transient.Update(ids[i], update_vector.data + i * size); });
	}
}

void VersionChunk::RetrieveTupleData(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids,
                                     index_t offset) {
	// check if this tuple is versioned
	index_t version_index = GetVersionIndex(offset);
	auto version = version_data[version_index];
	if (!version) {
		// not versioned, retrieve base data
		RetrieveTupleFromBaseTable(result, column_ids, start + offset);
		return;
	}
	index_t index_in_version = offset % STANDARD_VECTOR_SIZE;
	auto root_info = version->version_pointers[index_in_version];
	auto version_info = VersionInfo::GetVersionForTransaction(transaction, root_info);
	if (version_info) {
		if (version_info->tuple_data) {
			// tuple is versioned: retrieve the versioned data
			data_ptr_t alternate_version_pointer = version_info->tuple_data;
			index_t alternate_version_index = start + offset;

			table.RetrieveVersionedData(result, column_ids, &alternate_version_pointer, &alternate_version_index, 1);
		}
	} else {
		if (!version->deleted[index_in_version]) {
			// not versioned: retrieve info from base table
			RetrieveTupleFromBaseTable(result, column_ids, start + offset);
		}
	}
}

void VersionChunk::RetrieveColumnData(ColumnPointer &pointer, Vector &result, index_t count) {
	// copy data from the column storage
	while (count > 0) {
		// check how much we can copy from this column segment
		index_t to_copy = std::min(count, pointer.segment->count - pointer.offset);
		if (to_copy > 0) {
			// copy elements from the column segment
			pointer.segment->Scan(pointer, result, to_copy);
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

void VersionChunk::RetrieveColumnData(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector,
                                      index_t sel_count) {
	// copy data from the column storage
	while (count > 0) {
		// check how much we can copy from this column segment
		index_t to_copy = std::min(count, pointer.segment->count - pointer.offset);
		if (to_copy > 0) {
			// we can copy everything from this column segment, copy with the sel vector
			pointer.segment->Scan(pointer, result, to_copy, sel_vector, sel_count);
			count -= to_copy;
		}
		if (count > 0) {
			// we can't copy everything from this segment, move to the next segment
			assert(pointer.segment->next);
			pointer.segment = (ColumnSegment *)pointer.segment->next.get();
			pointer.offset = 0;
		}
	}
}

bool VersionChunk::Scan(TableScanState &state, Transaction &transaction, DataChunk &result,
                        const vector<column_t> &column_ids, index_t version_index) {
	// obtain a shared lock on this chunk
	auto shared_lock = lock.GetSharedLock();
	// now figure out how many tuples to scan in this chunk
	index_t scan_start = version_index * STANDARD_VECTOR_SIZE;
	index_t end = this == state.last_chunk ? state.last_chunk_count : this->count;
	index_t scan_count = min((index_t)STANDARD_VECTOR_SIZE, end - scan_start);
	if (scan_count == 0) {
		// exhausted this chunk already
		return true;
	}
	sel_t regular_entries[STANDARD_VECTOR_SIZE], version_entries[STANDARD_VECTOR_SIZE];
	index_t regular_count = 0, version_count = 0;

	// if the segment is dirty we need to scan the version pointers and deleted flags
	auto vdata = version_data[version_index];
	if (vdata) {
		// start scanning the chunk to check for deleted and version pointers
		for (index_t i = 0; i < scan_count; i++) {
			version_entries[version_count] = regular_entries[regular_count] = i;
			bool has_version = vdata->version_pointers[i];
			bool is_deleted = vdata->deleted[i];
			version_count += has_version;
			regular_count += !(is_deleted || has_version);
		}
		if (regular_count == scan_count && (scan_count == STANDARD_VECTOR_SIZE || end == this->count)) {
			// the scan was clean: delete the version_data
			version_data[version_index] = nullptr;
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
			auto root_info = vdata->version_pointers[version_entries[i]];
			// follow the version chain for this version
			auto version_info = VersionInfo::GetVersionForTransaction(transaction, root_info);
			if (!version_info) {
				// no version info available for this transaction: use base table data
				// check if entry was not deleted, if it was not deleted use the base table data
				if (!vdata->deleted[version_entries[i]]) {
					regular_entries[regular_count++] = version_entries[i];
				}
			} else {
				// version info available: use the version info
				if (version_info->tuple_data) {
					alternate_version_pointers[alternate_version_count] = version_info->tuple_data;
					alternate_version_index[alternate_version_count] = scan_start + version_entries[i];
					alternate_version_count++;
				}
			}
		}
		if (alternate_version_count > 0) {
			// retrieve alternate versions, if any
			table.RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index,
			                            alternate_version_count);
		}
		// retrieve entries from the base table with the selection vector
		FetchColumnData(state, result, column_ids, scan_start, scan_count, regular_entries, regular_count);
	} else {
		// no versions or deleted tuples, simply scan the column segments
		FetchColumnData(state, result, column_ids, scan_start, regular_count);
	}
	return scan_start + scan_count == end;
}

void VersionChunk::FetchColumnData(TableScanState &state, DataChunk &result, const vector<column_t> &column_ids,
                                   index_t offset_in_chunk, index_t scan_count, sel_t sel_vector[], index_t count) {
	for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
			assert(result.data[col_idx].type == TypeId::BIGINT);
			auto column_indexes = ((int64_t *)result.data[col_idx].data + result.data[col_idx].count);
			for (index_t i = 0; i < count; i++) {
				column_indexes[i] = this->start + offset_in_chunk + sel_vector[i];
			}
			result.data[col_idx].count += count;
		} else {
			// fetch the data from the base column segments
			RetrieveColumnData(state.columns[column_ids[col_idx]], result.data[col_idx], scan_count, sel_vector, count);
		}
	}
}

void VersionChunk::FetchColumnData(TableScanState &state, DataChunk &result, const vector<column_t> &column_ids,
                                   index_t offset_in_chunk, index_t count) {
	for (index_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		if (column_ids[col_idx] == COLUMN_IDENTIFIER_ROW_ID) {
			// generate column ids
			result.data[col_idx].count = count;
			VectorOperations::GenerateSequence(result.data[col_idx], this->start + offset_in_chunk, 1);
		} else {
			// fetch the data from the base column segments
			RetrieveColumnData(state.columns[column_ids[col_idx]], result.data[col_idx], count);
		}
	}
}

bool VersionChunk::CreateIndexScan(IndexTableScanState &state, vector<column_t> &column_ids, DataChunk &result) {
	if (state.offset == 0 && state.version_index == 0 && state.version_offset == 0 && !state.version_chain) {
		// first obtain a shared lock on the chunk, if we don't have it already
		state.locks.push_back(lock.GetSharedLock());
	}

	// first scan the base values
	while (state.offset < this->count) {
		sel_t regular_entries[STANDARD_VECTOR_SIZE];
		index_t regular_count = 0;
		index_t scan_count = min((index_t)STANDARD_VECTOR_SIZE, this->count - state.offset);
		index_t version_index = GetVersionIndex(state.offset);
		auto version = version_data[version_index];
		if (version) {
			// this chunk is versioned, scan to see which tuples are deleted
			for (index_t i = 0; i < scan_count; i++) {
				if (!version->deleted[i]) {
					regular_entries[regular_count++] = i;
				}
			}
		} else {
			regular_count = scan_count;
		}
		if (regular_count > 0) {
			if (regular_count == scan_count) {
				// no deleted tuples,get all data from the base columns
				FetchColumnData(state, result, column_ids, state.offset, regular_count);
			} else {
				FetchColumnData(state, result, column_ids, state.offset, scan_count, regular_entries, regular_count);
			}
		}
		state.offset += STANDARD_VECTOR_SIZE;
		if (result.size() > 0) {
			return false;
		}
	}
	index_t max_version_index = std::min((index_t)STORAGE_CHUNK_VECTORS, 1 + (this->count / STANDARD_VECTOR_SIZE));
	data_ptr_t alternate_version_pointers[STANDARD_VECTOR_SIZE];
	index_t alternate_version_index[STANDARD_VECTOR_SIZE];
	index_t result_count = 0;
	// the base table was exhausted, now scan any remaining version chunks
	while (state.version_index < max_version_index && result_count < STANDARD_VECTOR_SIZE) {
		auto version = version_data[state.version_index];
		if (!version) {
			state.version_index++;
			continue;
		}
		index_t remaining_count =
		    std::min((index_t)STANDARD_VECTOR_SIZE, this->count - state.version_index * STANDARD_VECTOR_SIZE);

		while (state.version_offset < remaining_count && result_count < STANDARD_VECTOR_SIZE) {
			if (!state.version_chain) {
				state.version_chain = version->version_pointers[state.version_offset];
			}

			// now chase the version pointer, if any
			while (state.version_chain && result_count < STANDARD_VECTOR_SIZE) {
				if (state.version_chain->tuple_data) {
					alternate_version_pointers[result_count] = state.version_chain->tuple_data;
					alternate_version_index[result_count] = state.version_chain->GetRowId();
					result_count++;
				}
				state.version_chain = state.version_chain->next;
			}
			if (!state.version_chain) {
				state.version_offset++;
			}
		}
		if (state.version_offset == remaining_count) {
			state.version_index++;
			state.version_offset = 0;
			state.version_chain = nullptr;
		}
	}
	if (result_count > 0) {
		table.RetrieveVersionedData(result, column_ids, alternate_version_pointers, alternate_version_index,
		                            result_count);
		return false;
	}
	return true;
}
