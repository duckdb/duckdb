#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "storage/data_table.hpp"
#include "transaction/version_info.hpp"

using namespace duckdb;
using namespace std;

VersionChunk::VersionChunk(DataTable &base_table, index_t start) : SegmentBase(start, 0), table(base_table) {
}

void VersionChunk::SetDirtyFlag(index_t start, index_t count, bool new_dirty_flag) {
	assert(count > 0 && count <= STANDARD_VECTOR_SIZE);
	assert(start + count <= STORAGE_CHUNK_SIZE);
	index_t marker_start = start / STANDARD_VECTOR_SIZE;
	index_t marker_end = (start + count - 1) / STANDARD_VECTOR_SIZE;
	for (index_t i = marker_start; i <= marker_end; i++) {
		assert(i < STORAGE_CHUNK_VECTORS);
		is_dirty[i] = new_dirty_flag;
	}
}

bool VersionChunk::IsDirty(index_t start, index_t count) {
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
