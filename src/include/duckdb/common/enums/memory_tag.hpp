//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/memory_tag.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class MemoryTag : uint8_t {
	BASE_TABLE = 0,
	HASH_TABLE = 1,
	PARQUET_READER = 2,
	CSV_READER = 3,
	ORDER_BY = 4,
	ART_INDEX = 5,
	COLUMN_DATA = 6,
	METADATA = 7,
	OVERFLOW_STRINGS = 8,
	IN_MEMORY_TABLE = 9,
	ALLOCATOR = 10,
	EXTENSION = 11
};

static constexpr const idx_t MEMORY_TAG_COUNT = 12;

} // namespace duckdb
