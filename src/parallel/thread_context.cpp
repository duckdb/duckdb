#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context)
    : profiler(QueryProfiler::Get(context).IsEnabled()), allocator(BufferAllocator::Get(context)) {
}

Allocator &ArenaAllocator::Get(ExecutionContext &context) {
	return Allocator::Get(context.client);
}

Allocator &ArenaAllocator::Get(ThreadContext &tcontext) {
	return tcontext.allocator.GetBatchedAllocator();
}

} // namespace duckdb
