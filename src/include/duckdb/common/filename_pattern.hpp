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

class Serializer;
class Deserializer;

class FilenamePattern {
	friend Deserializer;

public:
	FilenamePattern() : _base("data_"), _pos(_base.length()), _uuid(false) {
	}
	~FilenamePattern() {
	}

public:
	void SetFilenamePattern(const string &pattern);
	void SetExtension(const string &extension);
	string CreateFilename(FileSystem &fs, const string &path, idx_t offset) const;

	void Serialize(Serializer &serializer) const;
	static FilenamePattern Deserialize(Deserializer &deserializer);

private:
	string _base;
	string _extension;
	idx_t _pos;
	bool _uuid;
};

} // namespace duckdb
