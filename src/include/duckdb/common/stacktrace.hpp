//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/stacktrace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#ifdef DUCKDB_CPPTRACE
#include <cpptrace/cpptrace.hpp>
#endif

namespace duckdb {

class StackTrace {
public:
	static string GetStacktracePointers(idx_t max_depth = 120);
	static string ResolveStacktraceSymbols(const string &pointers);

	inline static string GetStackTrace(idx_t max_depth = 120) {
#ifdef DUCKDB_CPPTRACE
		return cpptrace::generate_trace().to_string(false);
#else
		return ResolveStacktraceSymbols(GetStacktracePointers(max_depth));
#endif
	}
};

} // namespace duckdb
