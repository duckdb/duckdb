//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/stacktrace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class StackTrace {
public:
	static string GetStacktracePointers(idx_t max_depth = 120);
	static string ResolveStacktraceSymbols(const string &pointers);

	inline static string GetStackTrace(idx_t max_depth = 120) {
		return ResolveStacktraceSymbols(GetStacktracePointers(max_depth));
	}
};

} // namespace duckdb
