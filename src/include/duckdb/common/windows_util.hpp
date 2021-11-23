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
	static wstring WindowsUTF8ToUnicode(const char *input);
	static wstring WindowsUTF8ToUnicode(const string &input);
	static string WindowsUnicodeToUTF8(LPCWSTR input);
};
#endif

} // namespace duckdb
