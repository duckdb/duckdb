#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

PendingQueryResult::PendingQueryResult(shared_ptr<ClientContext> context_p, PreparedStatementData &statement,
                                       vector<LogicalType> types_p, bool allow_stream_result)
    : BaseQueryResult(QueryResultType::PENDING_RESULT, statement.statement_type, statement.properties,
                      std::move(types_p), statement.names),
      context(std::move(context_p)), allow_stream_result(allow_stream_result) {
}

PendingQueryResult::PendingQueryResult(ErrorData error)
    : BaseQueryResult(QueryResultType::PENDING_RESULT, std::move(error)) {
}

PendingQueryResult::~PendingQueryResult() {
}

ClientContext &PendingQueryResult::GetClientContext() const {
	if (!context) {
		if (HasError()) {
			throw InvalidInputException(
			    "Attempting to execute an unsuccessful or closed pending query result\nError: %s", GetError());
		}
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result");
	}
	return *context;
}

void PendingQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	bool invalidated = HasError() || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, *this);
	}
	if (invalidated) {
		if (HasError()) {
			throw InvalidInputException(
			    "Attempting to execute an unsuccessful or closed pending query result\nError: %s", GetError());
		}
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result");
	}
}

void PendingQueryResult::WaitForTask() {
	ClientContextLock lock(GetClientContext());
	context->WaitForTask(lock, *this);
}

PendingExecutionResult PendingQueryResult::ExecuteTask() {
	ClientContextLock lock(GetClientContext());
	return ExecuteTaskInternal(lock);
}

PendingExecutionResult PendingQueryResult::CheckPulse() {
	ClientContextLock lock(GetClientContext());
	CheckExecutableInternal(lock);
	return context->ExecuteTaskInternal(lock, *this, true);
}

bool PendingQueryResult::AllowStreamResult() const {
	return allow_stream_result;
}

PendingExecutionResult PendingQueryResult::ExecuteTaskInternal(ClientContextLock &lock) {
	CheckExecutableInternal(lock);
	return context->ExecuteTaskInternal(lock, *this, false);
}

unique_ptr<QueryResult> PendingQueryResult::ExecuteInternal(ClientContextLock &lock) {
	CheckExecutableInternal(lock);

	PendingExecutionResult execution_result;
	while (!IsResultReady(execution_result = ExecuteTaskInternal(lock))) {
		if (execution_result == PendingExecutionResult::BLOCKED) {
			CheckExecutableInternal(lock);
			context->WaitForTask(lock, *this);
		}
	}
	if (HasError()) {
		if (allow_stream_result) {
			return make_uniq<StreamQueryResult>(GetErrorObject());
		} else {
			return make_uniq<MaterializedQueryResult>(GetErrorObject());
		}
	}
	auto result = context->FetchResultInternal(lock, *this);
	// release our context reference (cannot use Close(): the context lock is already held here)
	context.reset();
	return result;
}

unique_ptr<QueryResult> PendingQueryResult::Execute() {
	ClientContextLock lock(GetClientContext());
	return ExecuteInternal(lock);
}

void PendingQueryResult::Close() {
	if (context) {
		ClientContextLock lock(GetClientContext());
		if (context->IsActiveResult(lock, *this)) {
			// Abandoned before execution finished: release the active-query state now (matching
			// InitialCleanup) instead of leaking it until the next query or context teardown.
			context->CleanupInternal(lock, this, false);
		}
	}
	context.reset();
}

bool PendingQueryResult::IsResultReady(PendingExecutionResult result) {
	return (IsExecutionFinished(result) || result == PendingExecutionResult::RESULT_READY);
}

bool PendingQueryResult::IsExecutionFinished(PendingExecutionResult result) {
	return (result == PendingExecutionResult::EXECUTION_FINISHED || result == PendingExecutionResult::EXECUTION_ERROR);
}

} // namespace duckdb
