//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/windows_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/windows.hpp"

namespace duckdb {

#ifdef DUCKDB_WINDOWS
class WindowsUtil {
public:
	//! Windows helper functions
	static std::wstring UTF8ToUnicode(const char *input);
	static string UnicodeToUTF8(LPCWSTR input);
	static string UTF8ToMBCS(const char *input, bool use_ansi = false);
};
#endif

} // namespace duckdb
