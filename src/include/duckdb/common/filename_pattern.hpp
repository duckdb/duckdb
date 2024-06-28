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
	FilenamePattern() : base("data_"), pos(base.length()), uuid(false) {
	}

public:
	void SetFilenamePattern(const string &pattern);
	string CreateFilename(FileSystem &fs, const string &path, const string &extension, idx_t offset) const;

	void Serialize(Serializer &serializer) const;
	static FilenamePattern Deserialize(Deserializer &deserializer);

	bool HasUUID() const {
		return uuid;
	}

private:
	string base;
	idx_t pos;
	bool uuid;
};

} // namespace duckdb
