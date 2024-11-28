//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/thread_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_profiler.hpp"

namespace duckdb {
class ClientContext;
class Logger;

//! The ThreadContext holds thread-local info for parallel usage
class ThreadContext {
public:
	explicit ThreadContext(ClientContext &context);
	~ThreadContext();

	//! The operator profiler for the individual thread context
	OperatorProfiler profiler;
	unique_ptr<Logger> logger;
};

} // namespace duckdb
