#include "duckdb/parallel/thread_context.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context) : profiler(context.profiler->IsEnabled()) {
}

} // namespace duckdb
