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

public:
	FilenameFormatCreator() : _base("data_"), _pos(_base.length()), _uuid(false) {
	}
	~FilenameFormatCreator() {
	} // dellete?

public:
	void SetFilenameFormat(const string &format);
	string CreateFilename(const FileSystem &fs, const string &path, const string &extension, idx_t offset) const;

private:
	string _base;
	idx_t _pos;
	bool _uuid;
};

} // namespace duckdb
