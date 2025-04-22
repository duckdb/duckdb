//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/open_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ExtendedOpenFileInfo {
	unordered_map<string, Value> options;
};

struct OpenFileInfo {
	OpenFileInfo() = default;
	OpenFileInfo(string path_p) // NOLINT: allow implicit conversion from string
	    : path(std::move(path_p)) {
	}

	string path;
	shared_ptr<ExtendedOpenFileInfo> extended_info;

public:
	bool operator<(const OpenFileInfo &rhs) const {
		return path < rhs.path;
	}
};

} // namespace duckdb
