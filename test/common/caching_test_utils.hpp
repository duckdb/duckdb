//===----------------------------------------------------------------------===//
//                         DuckDB
//
// Shared test utilities for external file cache tests.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "test_helpers.hpp"

namespace duckdb {

class CachingTestFileGuard {
public:
	CachingTestFileGuard(const string &filename, const string &content);
	~CachingTestFileGuard();

	const string &GetPath() const;

private:
	string file_path;
};

class SimpleTrackingFileSystem : public LocalFileSystem {
public:
	string GetName() const override;
	bool CanHandleFile(const string &path) override;
	bool CanSeek() override;
	string GetVersionTag(FileHandle &handle) override;
};

//! A file system that returns no ETag and timestamp_t(0) for Last-Modified, simulating servers that do not
//! provide cache-validation headers.
class NoValidationMetadataFileSystem : public LocalFileSystem {
public:
	string GetName() const override;
	bool CanHandleFile(const string &path) override;
	bool CanSeek() override;
	string GetVersionTag(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
};

//! In-memory DuckDB with the external file cache forced to also cache local files (off by default), so the external
//! file cache tests can exercise the cache machinery on local temp files.
DuckDB MakeCacheLocalFilesDB();

} // namespace duckdb
