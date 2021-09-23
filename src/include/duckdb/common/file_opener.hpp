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

class Value;

//! Abstact type that provide client-spcific context to FileSystem.
class FileOpener {
public:
	virtual ~FileOpener() {};

	virtual bool TryGetCurrentSetting(const string &key, Value &result) = 0;
};

} // namespace duckdb