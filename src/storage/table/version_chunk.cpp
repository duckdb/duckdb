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

VersionChunkInfo *VersionChunk::GetVersionInfo(index_t index) {
	index_t version_index = GetVersionIndex(index);
	return GetOrCreateVersionInfo(version_index);
}

VersionChunkInfo *VersionChunk::GetOrCreateVersionInfo(index_t version_index) {
	assert(version_index < STORAGE_CHUNK_VECTORS);
	if (!version_data[version_index]) {
		version_data[version_index] = make_shared<VersionChunkInfo>(*this, version_index * STANDARD_VECTOR_SIZE);
	}
	return version_data[version_index].get();
}

void VersionChunk::PushDeletedEntries(Transaction &transaction, transaction_t commit_id, index_t amount) {
	index_t version_index = GetVersionIndex(this->count);
	index_t offset_in_version = this->count % STANDARD_VECTOR_SIZE;

	auto version = GetOrCreateVersionInfo(version_index);
	for (index_t i = 0; i < amount; i++) {
		version->inserted[offset_in_version] = commit_id;
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

	bool is_inserted = Versioning::UseVersion(transaction, version->inserted[index_in_version]);
	bool is_deleted = Versioning::UseVersion(transaction, version->deleted[index_in_version]);
	if (is_inserted && !is_deleted) {
		// not versioned: retrieve info from base table
		RetrieveTupleFromBaseTable(result, column_ids, start + offset);
	}
}

void VersionChunk::RetrieveColumnData(ColumnPointer &pointer, Vector &result, index_t count) {
	// copy data from the column storage
	pointer.segment->InitializeScan(pointer);
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
	pointer.segment->InitializeScan(pointer);
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
	sel_t regular_entries[STANDARD_VECTOR_SIZE];
	index_t regular_count = 0;

	// if the segment is dirty we need to scan the version pointers and deleted flags
	auto vdata = version_data[version_index];
	if (vdata) {
		// start scanning the chunk to check for deleted and version pointers
		for (index_t i = 0; i < scan_count; i++) {
			regular_entries[regular_count] = i;
			bool is_inserted = Versioning::UseVersion(transaction, vdata->inserted[i]);
			bool is_deleted = Versioning::UseVersion(transaction, vdata->deleted[i]);
			regular_count += is_inserted && !is_deleted;
		}
	} else {
		// no deleted entries or version information: just scan everything
		regular_count = scan_count;
	}

	if (regular_count < scan_count) {
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
	throw Exception("FIXME: create index scan");
}
