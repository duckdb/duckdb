//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/file_glob_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include <limits>

#undef max

namespace duckdb {

struct HiveFilterParams;

enum class FileGlobOptions : uint8_t { DISALLOW_EMPTY = 0, ALLOW_EMPTY = 1, FALLBACK_GLOB = 2 };

struct FileGlobInput {
	FileGlobInput(FileGlobOptions options) // NOLINT: allow implicit conversion from FileGlobOptions
	    : behavior(options), max_files(std::numeric_limits<idx_t>::max()) {
	}
	FileGlobInput(FileGlobOptions options, string extension_p)
	    : behavior(options), extension(std::move(extension_p)), max_files(std::numeric_limits<idx_t>::max()) {
	}

	FileGlobOptions behavior;
	string extension;
	idx_t max_files;
	optional_ptr<HiveFilterParams> hive_params;
};

} // namespace duckdb
