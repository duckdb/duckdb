#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

PendingQueryResult::PendingQueryResult(shared_ptr<ClientContext> context_p, PreparedStatementData &statement,
                                       vector<LogicalType> types_p)
    : BaseQueryResult(QueryResultType::PENDING_RESULT, statement.statement_type, move(types_p), statement.names),
      context(move(context_p)) {
}

PendingQueryResult::PendingQueryResult(string error) : BaseQueryResult(QueryResultType::PENDING_RESULT, move(error)) {
}

PendingQueryResult::~PendingQueryResult() {
}

unique_ptr<ClientContextLock> PendingQueryResult::LockContext() {
	if (!context) {
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result\nError: %s",
		                            error);
	}
	return context->LockContext();
}

void PendingQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	bool invalidated = !success || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, this);
	}
	if (invalidated) {
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result\nError: %s",
		                            error);
	}
}

PendingExecutionResult PendingQueryResult::ExecuteTask() {
	auto lock = LockContext();
	return ExecuteTaskInternal(*lock);
}

PendingExecutionResult PendingQueryResult::ExecuteTaskInternal(ClientContextLock &lock) {
	CheckExecutableInternal(lock);
	return context->ExecuteTaskInternal(lock, *this);
}

unique_ptr<QueryResult> PendingQueryResult::ExecuteInternal(ClientContextLock &lock, bool allow_streaming_result) {
	CheckExecutableInternal(lock);
	while (ExecuteTaskInternal(lock) == PendingExecutionResult::RESULT_NOT_READY) {
	}
	if (!success) {
		return make_unique<MaterializedQueryResult>(error);
	}
	auto result = context->FetchResultInternal(lock, *this, allow_streaming_result);
	Close();
	return result;
}

unique_ptr<QueryResult> PendingQueryResult::Execute(bool allow_streaming_result) {
	auto lock = LockContext();
	return ExecuteInternal(*lock, allow_streaming_result);
}

void PendingQueryResult::Close() {
	context.reset();
}

} // namespace duckdb
