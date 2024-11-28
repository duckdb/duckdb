#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context) : profiler(context) {
	LoggingContext log_context;
	log_context.default_log_type = "thread_context";
	// TODO: set to not thread safe
	logger = context.db->GetLogManager().CreateLogger(log_context, true);
}

 ThreadContext::~ThreadContext() {
}


} // namespace duckdb
