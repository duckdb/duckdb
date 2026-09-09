#include "duckdb/main/query_result_stream.hpp"

#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

QueryResultStream::QueryResultStream(unique_ptr<QueryResult> result) : handle(std::move(result)) {
	if (!handle) {
		throw InvalidInputException("Attempting to open a stream on a query result that does not exist");
	}
	if (handle->HasError()) {
		throw InvalidInputException("Attempting to open a stream on an unsuccessful query result\nError: %s",
		                            handle->GetError());
	}
	if (handle->GetStatementProperties().complete_on_return) {
		throw InvalidInputException(
		    "Attempting to open a stream on a %s statement, which completes before its result is returned",
		    StatementTypeToString(handle->GetStatementType()));
	}
	if (!handle->HasBufferedData()) {
		throw InvalidInputException("Attempting to open a stream on a query result that has no streaming buffer");
	}
	if (handle->GetBufferedData().Decide(ResultLifetime::DRAINING) != ResultLifetime::DRAINING) {
		throw InvalidInputException("Attempting to open a stream on a query result that is being retained");
	}
}

QueryResultStream::~QueryResultStream() {
	Close();
}

void QueryResultStream::Close() {
	handle->Close();
}

bool QueryResultStream::IsOpen() {
	return handle->IsOpen();
}

QueryResultState QueryResultStream::Poll() {
	return handle->Poll();
}

QueryResultState QueryResultStream::ExecuteTask() {
	if (!handle->context) {
		// The stream already ended. Keep reporting the terminal state
		return handle->HasError() ? QueryResultState::EXECUTION_ERROR : QueryResultState::FINISHED;
	}
	QueryResultState state;
	{
		auto lock = handle->LockContext();
		try {
			state = handle->buffer->ExecuteTaskInternal(*handle, *lock);
		} catch (std::exception &ex) {
			// A pending interrupt reaches the consumer as an error on the stream, never as a throw
			handle->HandleFetchFailure(*lock, ErrorData(ex));
			state = QueryResultState::EXECUTION_ERROR;
		} catch (...) { // LCOV_EXCL_START
			handle->SetError(ErrorData("Unhandled exception in ExecuteTask"));
			handle->EndQuery(*lock, true);
			state = QueryResultState::EXECUTION_ERROR;
		} // LCOV_EXCL_STOP
	}
	if (state == QueryResultState::EXECUTION_ERROR) {
		// A finished execution can still hold trailing chunks, so only an error ends the stream here
		Close();
	}
	return state;
}

void QueryResultStream::WaitForTask() {
	if (!handle->context) {
		return;
	}
	handle->buffer->UnblockSinks();
	handle->WaitForTask();
}

QueryResultState QueryResultStream::TryFetch(unique_ptr<DataChunk> &out_chunk) {
	out_chunk.reset();
	if (!handle->context) {
		// The stream already ended. Keep reporting the terminal state
		return handle->HasError() ? QueryResultState::EXECUTION_ERROR : QueryResultState::FINISHED;
	}
	auto &buffer = *handle->buffer;
	QueryResultState state;
	{
		auto lock = handle->LockContext();
		try {
			state = buffer.Pulse(*handle, *lock);
			if (state != QueryResultState::EXECUTION_ERROR) {
				if (state == QueryResultState::READY) {
					out_chunk = buffer.Scan();
				}
				if (out_chunk && out_chunk->size() != 0) {
					return QueryResultState::READY;
				}
				out_chunk.reset();
				if (state == QueryResultState::FINISHED) {
					// The buffer is drained and execution is done: this is the end of the stream
					buffer.AssertNoBlockedSinks();
					handle->EndQuery(*lock);
					// Cleanup can fail on an autocommit commit. It records the error without throwing
					state = handle->HasError() ? QueryResultState::EXECUTION_ERROR : QueryResultState::FINISHED;
				} else if (state == QueryResultState::READY) {
					// A chunk was announced but the scan came up empty: the stream has not ended yet
					state = QueryResultState::NOT_READY;
				}
			}
		} catch (std::exception &ex) {
			handle->HandleFetchFailure(*lock, ErrorData(ex));
			state = QueryResultState::EXECUTION_ERROR;
		} catch (...) { // LCOV_EXCL_START
			handle->SetError(ErrorData("Unhandled exception in TryFetch"));
			handle->EndQuery(*lock, true);
			state = QueryResultState::EXECUTION_ERROR;
		} // LCOV_EXCL_STOP
	}
	if (IsTerminal(state)) {
		Close();
	}
	return state;
}

unique_ptr<DataChunk> QueryResultStream::FetchInternal(ClientContextLock &lock) {
	auto &buffer = *handle->buffer;
	unique_ptr<DataChunk> chunk;
	try {
		auto state = buffer.ReplenishBuffer(*handle, lock);
		if (state == QueryResultState::EXECUTION_ERROR) {
			return nullptr;
		}
		chunk = buffer.Scan();
		if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
			handle->EndQuery(lock);
			return nullptr;
		}
		return chunk;
	} catch (std::exception &ex) {
		handle->HandleFetchFailure(lock, ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		handle->SetError(ErrorData("Unhandled exception in Fetch"));
		handle->EndQuery(lock, true);
	} // LCOV_EXCL_STOP
	return nullptr;
}

unique_ptr<DataChunk> QueryResultStream::Fetch() {
	if (!handle->context && !handle->HasError()) {
		// The stream ended cleanly. Keep reporting the end, the way TryFetch and Poll do
		return nullptr;
	}
	unique_ptr<DataChunk> chunk;
	{
		auto lock = handle->LockContext();
		handle->CheckExecutableInternal(*lock);
		chunk = FetchInternal(*lock);
	}
	if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
		if (!HasError()) {
			handle->buffer->AssertNoBlockedSinks();
		}
		Close();
		return nullptr;
	}
	chunk->Flatten();
	return chunk;
}

void QueryResultStream::SetError(ErrorData error) {
	handle->SetError(std::move(error));
}

bool QueryResultStream::HasError() const {
	return handle->HasError();
}

const string &QueryResultStream::GetError() const {
	return handle->GetError();
}

const ErrorData &QueryResultStream::GetErrorObject() const {
	return handle->GetErrorObject();
}

const ExceptionType &QueryResultStream::GetErrorType() const {
	return handle->GetErrorType();
}

const vector<LogicalType> &QueryResultStream::GetTypes() const {
	return handle->GetTypes();
}

const vector<Identifier> &QueryResultStream::GetNames() const {
	return handle->GetNames();
}

const Identifier &QueryResultStream::ColumnName(idx_t index) const {
	return handle->ColumnName(index);
}

idx_t QueryResultStream::ColumnCount() const {
	return handle->ColumnCount();
}

StatementType QueryResultStream::GetStatementType() const {
	return handle->GetStatementType();
}

const StatementProperties &QueryResultStream::GetStatementProperties() const {
	return handle->GetStatementProperties();
}

const ClientProperties &QueryResultStream::GetClientProperties() const {
	return handle->client_properties;
}

ClientProperties &QueryResultStream::GetClientProperties() {
	return handle->client_properties;
}

} // namespace duckdb
