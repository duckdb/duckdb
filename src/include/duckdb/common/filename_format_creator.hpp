//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/filename_format_creator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

class FilenameFormatCreator {
	typedef uint64_t idx_t;

public:
	FilenameFormatCreator() : _base("data_"), _pos(_base.length()), _uuid(false) {
	}
	~FilenameFormatCreator() {
	}

public:
	void setFilenameFormat(const string &format);
	string CreateFilename(const FileSystem &fs, const string &path, const string &extension, idx_t offset) const;

private:
	string _base;
	idx_t _pos;
	bool _uuid;
};

} // namespace duckdb
