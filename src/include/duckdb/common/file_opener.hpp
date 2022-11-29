//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

class ClientContext;
class Value;

//! Abstract type that provide client-specific context to FileSystem.
class FileOpener {
public:
	DUCKDB_API virtual ~FileOpener() {};

	DUCKDB_API virtual bool TryGetCurrentSetting(const string &key, Value &result) = 0;

	DUCKDB_API static FileOpener *Get(ClientContext &context);
};

} // namespace duckdb
