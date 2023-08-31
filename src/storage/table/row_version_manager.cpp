#include "duckdb/storage/table/row_version_manager.hpp"

namespace duckdb {


void RowVersionManager::SetStart(idx_t start) {
	idx_t current_start = start;
	for (idx_t i = 0; i < Storage::ROW_GROUP_VECTOR_COUNT; i++) {
		if (info[i]) {
			info[i]->start = current_start;
		}
		current_start += STANDARD_VECTOR_SIZE;
	}
}

idx_t RowVersionManager::GetCommittedDeletedCount(idx_t count) {
	idx_t deleted_count = 0;
	for (idx_t r = 0, i = 0; r < count; r += STANDARD_VECTOR_SIZE, i++) {
		if (!info[i]) {
			continue;
		}
		idx_t max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - r);
		if (max_count == 0) {
			break;
		}
		deleted_count += info[i]->GetCommittedDeletedCount(max_count);
	}
	return deleted_count;
}

}
