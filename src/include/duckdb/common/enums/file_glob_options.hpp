//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/file_glob_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class FileGlobOptions : uint8_t {
	DISALLOW_EMPTY = 0,
	ALLOW_EMPTY = 1,
};

} // namespace duckdb
