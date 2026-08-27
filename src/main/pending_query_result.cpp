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
      context(std::move(context_p)), allow_stream_result(allow_stream_result),
      async(statement.execution_mode == StreamingExecutionMode::ASYNC) {
}

PendingQueryResult::PendingQueryResult(ErrorData error)
    : BaseQueryResult(QueryResultType::PENDING_RESULT, std::move(error)), allow_stream_result(false), async(false) {
}

PendingQueryResult::~PendingQueryResult() {
}

unique_ptr<ClientContextLock> PendingQueryResult::LockContext() {
	if (!context) {
		if (HasError()) {
			throw InvalidInputException(
			    "Attempting to execute an unsuccessful or closed pending query result\nError: %s", GetError());
		}
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result");
	}
	return context->LockContext();
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
	auto lock = LockContext();
	if (async) {
		// An async caller never runs tasks, so wait for worker progress. WaitForTask
		// returns immediately whenever the producer queue is non-empty.
		context->WaitForProgress(*lock, *this);
		return;
	}
	context->WaitForTask(*lock, *this);
}

PendingExecutionResult PendingQueryResult::ExecuteTask() {
	auto lock = LockContext();
	return ExecuteTaskInternal(*lock);
}

PendingExecutionResult PendingQueryResult::CheckPulse() {
	auto lock = LockContext();
	CheckExecutableInternal(*lock);
	return context->ExecuteTaskInternal(*lock, *this, true);
}

bool PendingQueryResult::AllowStreamResult() const {
	return allow_stream_result;
}

PendingExecutionResult PendingQueryResult::ExecuteTaskInternal(ClientContextLock &lock) {
	CheckExecutableInternal(lock);
	// An async query never runs tasks on the consumer thread. The notify callback
	// could otherwise run on this thread's stack under the context lock
	return context->ExecuteTaskInternal(lock, *this, async);
}

unique_ptr<QueryResult> PendingQueryResult::ExecuteInternal(ClientContextLock &lock) {
	CheckExecutableInternal(lock);

	PendingExecutionResult execution_result;
	while (!IsResultReady(execution_result = ExecuteTaskInternal(lock))) {
		if (async) {
			CheckExecutableInternal(lock);
			if (context->IsInterrupted()) {
				// Observing without running tasks cannot surface interrupts. The executing
				// call throws on its interrupt guard before fetching any task, and its catch
				// converts the interrupt into an error result exactly like the sync path.
				execution_result = context->ExecuteTaskInternal(lock, *this, false);
				break;
			}
			// An async consumer makes no progress itself, so wait for the workers here.
			// WaitForTask would return immediately whenever the producer queue is non-empty.
			context->WaitForProgress(lock, *this);
		} else if (execution_result == PendingExecutionResult::BLOCKED) {
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
	auto lock = LockContext();
	return ExecuteInternal(*lock);
}

void PendingQueryResult::Close() {
	if (context) {
		auto lock = LockContext();
		if (context->IsActiveResult(*lock, *this)) {
			// Abandoned before execution finished: release the active-query state now (matching
			// InitialCleanup) instead of leaking it until the next query or context teardown.
			context->CleanupInternal(*lock, this, false);
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
