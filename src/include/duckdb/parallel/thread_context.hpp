//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/thread_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_profiler.hpp"
#include "duckdb/storage/batched_allocator.hpp"

namespace duckdb {
class ClientContext;

//! The ThreadContext holds thread-local info for parallel usage
class ThreadContext {
public:
	explicit ThreadContext(ClientContext &context);

	//! The operator profiler for the individual thread context
	OperatorProfiler profiler;
	//! Thread-local batch allocator
	BatchedAllocator allocator;
};

} // namespace duckdb
