#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context) : profiler(QueryProfiler::Get(context).IsEnabled(), ClientConfig::GetConfig(context).profiler_settings) {
 }

} // namespace duckdb
