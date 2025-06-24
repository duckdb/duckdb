//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/copy_overwrite_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class CopyOverwriteMode : uint8_t {
	COPY_ERROR_ON_CONFLICT = 0,
	COPY_OVERWRITE = 1,
	COPY_OVERWRITE_OR_IGNORE = 2,
	COPY_APPEND = 3
};

} // namespace duckdb
