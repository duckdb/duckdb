#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "storage/data_table.hpp"
#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

StorageChunk::StorageChunk(DataTable &base_table, index_t start) : SegmentBase(start, 0), table(base_table) {
}

void StorageChunk::Cleanup(VersionInformation *info) {
	index_t entry = info->prev.entry;
	version_pointers[entry] = info->next;
	if (version_pointers[entry]) {
		version_pointers[entry]->prev.entry = entry;
		version_pointers[entry]->chunk = this;
	}
}

void StorageChunk::Undo(VersionInformation *info) {
	index_t entry = info->prev.entry;
	assert(version_pointers[entry] == info);
	if (!info->tuple_data) {
		deleted[entry] = true;
	} else {
		// move data back to the original chunk
		deleted[entry] = false;
		auto tuple_data = info->tuple_data;

		vector<data_ptr_t> data_pointers;
		for (index_t i = 0; i < table.types.size(); i++) {
			data_pointers.push_back(GetPointerToRow(i, start + entry));
		}
		table.serializer.Deserialize(data_pointers, 0, tuple_data);
	}
	version_pointers[entry] = info->next;
	if (version_pointers[entry]) {
		version_pointers[entry]->prev.entry = entry;
		version_pointers[entry]->chunk = this;
	}
}

data_ptr_t StorageChunk::GetPointerToRow(index_t col, index_t row) {
	return columns[col].segment->GetPointerToRow(table.types[col], row);
}

void StorageChunk::SetDirtyFlag(index_t start, index_t count, bool new_dirty_flag) {
	assert(count > 0 && count <= STANDARD_VECTOR_SIZE);
	assert(start + count <= STORAGE_CHUNK_SIZE);
	index_t marker_start = start / STANDARD_VECTOR_SIZE;
	index_t marker_end = (start + count - 1) / STANDARD_VECTOR_SIZE;
	for (index_t i = marker_start; i <= marker_end; i++) {
		assert(i < STORAGE_CHUNK_VECTORS);
		is_dirty[i] = new_dirty_flag;
	}
}

bool StorageChunk::IsDirty(index_t start, index_t count) {
	assert(count > 0 && count <= STANDARD_VECTOR_SIZE);
	assert(start + count <= STORAGE_CHUNK_SIZE);
	index_t marker_start = start / STANDARD_VECTOR_SIZE;
	index_t marker_end = (start + count - 1) / STANDARD_VECTOR_SIZE;
	bool segment_is_dirty = false;
	for (index_t i = marker_start; i <= marker_end; i++) {
		assert(i < STORAGE_CHUNK_VECTORS);
		segment_is_dirty = segment_is_dirty || is_dirty[i];
	}
	return segment_is_dirty;
}
