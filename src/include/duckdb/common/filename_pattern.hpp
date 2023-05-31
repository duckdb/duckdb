//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/filename_pattern.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

class FilenamePattern {

public:
	FilenamePattern() : _base("data_"), _pos(_base.length()), _uuid(false) {
	}
	~FilenamePattern() {
	}

public:
	void SetFilenamePattern(const string &pattern);
	string CreateFilename(FileSystem &fs, const string &path, const string &extension, idx_t offset) const;

private:
	string _base;
	idx_t _pos;
	bool _uuid;
};

} // namespace duckdb
