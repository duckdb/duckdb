#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context) : profiler(context) {
	LoggingContext log_context(LogContextScope::THREAD);
	log_context.client_context = reinterpret_cast<idx_t>(&context);
	log_context.thread = TaskScheduler::GetEstimatedCPUId();
	if (context.transaction.HasActiveTransaction()) {
		log_context.transaction_id = context.transaction.GetActiveQuery();
	}
	logger = context.db->GetLogManager().CreateLogger(log_context, true);
}

ThreadContext::~ThreadContext() {
}

} // namespace duckdb
