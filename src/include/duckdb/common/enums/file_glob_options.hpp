//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/file_glob_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

struct HiveFilterParams;

enum class FileGlobOptions : uint8_t { DISALLOW_EMPTY = 0, ALLOW_EMPTY = 1, FALLBACK_GLOB = 2 };

struct FileGlobInput {
	FileGlobInput(FileGlobOptions options) // NOLINT: allow implicit conversion from FileGlobOptions
	    : behavior(options) {
	}
	FileGlobInput(FileGlobOptions options, string extension_p, optional_idx min_files = optional_idx())
	    : behavior(options), extension(std::move(extension_p)), min_files(min_files) {
	}

	virtual ~FileGlobInput() = default;

	//! Whether or not to include this file in the final glob
	virtual bool IncludeFile(const OpenFileInfo &file) const {
		return true;
	}

	//! Called after globbing is complete
	virtual void Finalize() const {
		return;
	}

	FileGlobOptions behavior;
	string extension;
	//! Minimum number of files to fetch before returning
	optional_idx min_files;
};

} // namespace duckdb
