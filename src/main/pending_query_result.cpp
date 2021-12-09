#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

PendingQueryResult::PendingQueryResult(shared_ptr<ClientContext> context_p, shared_ptr<PreparedStatementData> statement_p, unique_ptr<Executor> executor_p, vector<LogicalType> types_p) :
	BaseQueryResult(QueryResultType::PENDING_RESULT, statement_p->statement_type, move(types_p), statement_p->names), context(move(context_p)), statement(move(statement_p)), is_open(true), executor(move(executor_p)) {}

PendingQueryResult::PendingQueryResult(string error) :
	BaseQueryResult(QueryResultType::PENDING_RESULT, move(error)) {

}

PendingQueryResult::~PendingQueryResult() {}

void PendingQueryResult::CheckExecutable() {
	if (!success || !is_open) {
		throw InvalidInputException(
		    "Attempting to execute an unsuccessful or closed pending query result\nError: %s", error);
	}
}

PendingExecutionResult PendingQueryResult::ExecuteTask() {
	CheckExecutable();
	return PendingExecutionResult::RESULT_READY;
}

unique_ptr<QueryResult> PendingQueryResult::ExecuteInternal(ClientContextLock &lock, bool allow_streaming_result) {
	CheckExecutable();
	while(ExecuteTask() == PendingExecutionResult::RESULT_NOT_READY);
	if (!success) {
		return make_unique<MaterializedQueryResult>(error);
	}
	return context->FetchResultInternal(lock, *this, allow_streaming_result);
}

unique_ptr<QueryResult> PendingQueryResult::Execute(bool allow_streaming_result) {
	CheckExecutable();
	auto lock = context->LockContext();
	return ExecuteInternal(*lock, allow_streaming_result);
}

void PendingQueryResult::Close() {
	context.reset();
	statement.reset();
	executor.reset();
	this->is_open = false;
}

void PendingQueryResult::MarkAsClosed() {
	this->is_open = false;
}

}
