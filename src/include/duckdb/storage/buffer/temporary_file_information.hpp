#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/memory_tag.hpp"

namespace duckdb {

struct MemoryInformation {
	MemoryTag tag;
	idx_t size;
};

struct TemporaryFileInformation {
	string path;
	idx_t size;
};

} // namespace duckdb
