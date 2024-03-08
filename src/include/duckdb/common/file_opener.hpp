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
#include "duckdb/main/settings.hpp"

namespace duckdb {

class ClientContext;
class Value;

struct FileOpenerInfo {
	string file_path;
};

//! Abstract type that provide client-specific context to FileSystem.
class FileOpener {
public:
	FileOpener() {
	}
	virtual ~FileOpener() {};

	virtual SettingLookupResult TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &info);
	virtual SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) = 0;
	virtual ClientContext *TryGetClientContext() = 0;

	DUCKDB_API static ClientContext *TryGetClientContext(FileOpener *opener);
	DUCKDB_API static SettingLookupResult TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result);
	DUCKDB_API static SettingLookupResult TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result,
	                                                           FileOpenerInfo &info);
};

} // namespace duckdb
