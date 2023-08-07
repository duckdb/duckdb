//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

class ClientContext;
class Value;

//! Abstract type that provide client-specific context to FileSystem.
class FileOpener {
public:
	FileOpener() {
	}
	virtual ~FileOpener() {};

	virtual bool TryGetCurrentSetting(const string &key, Value &result) = 0;
	virtual ClientContext *TryGetClientContext() = 0;

	virtual FileOpener *GetScopedOpener(const string &file_path) {
		return this;
	}

	DUCKDB_API static ClientContext *TryGetClientContext(FileOpener *opener);
	DUCKDB_API static bool TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result);
};

} // namespace duckdb
