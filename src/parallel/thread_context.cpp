#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context) : profiler(context) {
	LoggingContext log_context(LogContextScope::THREAD);

	log_context.connection_id = context.GetConnectionId();
	if (context.transaction.HasActiveTransaction()) {
		log_context.transaction_id = context.transaction.ActiveTransaction().global_transaction_id;
		auto query_id = context.transaction.GetActiveQuery();
		if (query_id == DConstants::INVALID_INDEX) {
			log_context.query_id = optional_idx();
		} else {
			log_context.query_id = query_id;
		}
	}

	log_context.thread_id = TaskScheduler::GetEstimatedCPUId();
	if (context.transaction.HasActiveTransaction()) {
		auto query_id = context.transaction.GetActiveQuery();
		if (query_id == DConstants::INVALID_INDEX) {
			log_context.transaction_id = optional_idx();
		} else {
			log_context.transaction_id = query_id;
		}
	}
	logger = context.db->GetLogManager().CreateLogger(log_context, true);
}

ThreadContext::~ThreadContext() {
}

} // namespace duckdb
