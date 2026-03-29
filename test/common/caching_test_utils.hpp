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

} // namespace duckdb
