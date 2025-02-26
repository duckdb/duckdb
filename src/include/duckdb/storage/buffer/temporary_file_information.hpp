//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/temporary_file_information.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/memory_tag.hpp"

namespace duckdb {

struct MemoryInformation {
	MemoryTag tag;
	idx_t size;
	idx_t evicted_data;
};

struct TemporaryFileInformation {
	string path;
	idx_t size;
};

struct CachedFileInformation {
	string path;
	idx_t nr_bytes;
	idx_t location;
	time_t last_modified;
	bool loaded;
};

} // namespace duckdb
