//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_lookup_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/open_file_info.hpp"

namespace duckdb {

//! The file an open, listing or request is for
struct FileLookupInfo {
	FileLookupInfo() = default;
	FileLookupInfo(string path) // NOLINT: allow implicit conversion from a path
	    : file(std::move(path)) {
	}
	FileLookupInfo(OpenFileInfo file_p) // NOLINT: allow implicit conversion from a file
	    : file(std::move(file_p)) {
	}

	//! The file, with the options it is opened with, e.g. the secret to open it with
	OpenFileInfo file;
};

} // namespace duckdb
