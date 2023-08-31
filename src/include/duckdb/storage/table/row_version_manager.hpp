//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_version_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

class RowVersionManager {
public:

	void SetStart(idx_t start);
	idx_t GetCommittedDeletedCount(idx_t count);

	// FIXME: make this private
	unique_ptr<ChunkInfo> info[Storage::ROW_GROUP_VECTOR_COUNT];
};

} // namespace duckdb
