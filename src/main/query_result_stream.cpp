#include "duckdb/main/query_result_stream.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/result_unit.hpp"

namespace duckdb {

ResultStreamBase::ResultStreamBase(unique_ptr<QueryResult> result, const char *expected_format)
    : handle(std::move(result)) {
	if (!handle) {
		throw InvalidInputException("Attempting to open a stream on a query result that does not exist");
	}
	if (handle->HasError()) {
		throw InvalidInputException("Attempting to open a stream on an unsuccessful query result\nError: %s",
		                            handle->GetError());
	}
	if (handle->GetStatementProperties().result_eagerness == ResultEagerness::FORCED) {
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
	if (!StringUtil::Equals(handle->Format().Name(), expected_format)) {
		throw InvalidInputException("Attempting to open a \"%s\" stream on a query result in the \"%s\" format",
		                            expected_format, handle->Format().Name());
	}
}

ResultStreamBase::~ResultStreamBase() {
	Close();
}

void ResultStreamBase::Close() {
	handle->Close();
}

bool ResultStreamBase::IsOpen() {
	return handle->IsOpen();
}

QueryResultState
ResultStreamBase::GuardedInternal(const char *name,
                                  const std::function<QueryResultState(ClientContextLock &lock)> &call) {
	if (!handle->context) {
		// The stream already ended. Keep reporting the terminal state
		return handle->HasError() ? QueryResultState::EXECUTION_ERROR : QueryResultState::FINISHED;
	}
	auto lock = handle->LockContext();
	try {
		return call(*lock);
	} catch (std::exception &ex) {
		// A pending interrupt reaches the consumer as an error on the stream, never as a throw
		handle->HandleFetchFailure(*lock, ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		handle->SetError(ErrorData(StringUtil::Format("Unhandled exception in %s", name)));
		handle->EndQuery(*lock, true);
	} // LCOV_EXCL_STOP
	return QueryResultState::EXECUTION_ERROR;
}

QueryResultState ResultStreamBase::Poll() {
	auto state = GuardedInternal("Poll", [&](ClientContextLock &lock) { return handle->buffer->Poll(lock, *handle); });
	if (state == QueryResultState::EXECUTION_ERROR) {
		Close();
	}
	return state;
}

QueryResultState ResultStreamBase::ExecuteTask() {
	auto state = GuardedInternal("ExecuteTask",
	                             [&](ClientContextLock &lock) { return handle->buffer->Participate(lock, *handle); });
	if (state == QueryResultState::EXECUTION_ERROR) {
		// A finished execution can still hold trailing units, so only an error ends the stream here
		Close();
	}
	return state;
}

void ResultStreamBase::WaitForTask() {
	if (!handle->context) {
		return;
	}
	handle->buffer->UnblockSinks();
	handle->WaitForTask();
}

QueryResultState ResultStreamBase::TryFetchUnit(unique_ptr<ResultUnit> &out_unit) {
	out_unit.reset();
	auto state = GuardedInternal("TryFetch", [&](ClientContextLock &lock) {
		auto &buffer = *handle->buffer;
		auto state = buffer.Poll(lock, *handle);
		if (state == QueryResultState::EXECUTION_ERROR) {
			return state;
		}
		if (state == QueryResultState::READY) {
			out_unit = buffer.Scan();
		}
		if (out_unit && out_unit->row_count != 0) {
			return QueryResultState::READY;
		}
		out_unit.reset();
		if (state == QueryResultState::FINISHED) {
			// The buffer is drained and execution is done: this is the end of the stream
			buffer.AssertNoBlockedSinks();
			handle->EndQuery(lock);
			// Cleanup can fail on an autocommit commit. It records the error without throwing
			return handle->HasError() ? QueryResultState::EXECUTION_ERROR : QueryResultState::FINISHED;
		}
		if (state == QueryResultState::READY) {
			// A unit was announced but the scan came up empty: the stream has not ended yet
			return QueryResultState::NOT_READY;
		}
		return state;
	});
	if (IsTerminal(state)) {
		Close();
	}
	return state;
}

unique_ptr<ResultUnit> ResultStreamBase::FetchUnitInternal(ClientContextLock &lock) {
	auto &buffer = *handle->buffer;
	try {
		auto state = buffer.ReplenishBuffer(lock, *handle);
		if (state == QueryResultState::EXECUTION_ERROR) {
			return nullptr;
		}
		auto unit = buffer.Scan();
		if (!unit || unit->row_count == 0) {
			handle->EndQuery(lock);
			return nullptr;
		}
		return unit;
	} catch (std::exception &ex) {
		handle->HandleFetchFailure(lock, ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		handle->SetError(ErrorData("Unhandled exception in Fetch"));
		handle->EndQuery(lock, true);
	} // LCOV_EXCL_STOP
	return nullptr;
}

unique_ptr<ResultUnit> ResultStreamBase::FetchUnit() {
	if (!handle->context && !handle->HasError()) {
		// The stream ended cleanly. Keep reporting the end, the way TryFetch and Poll do
		return nullptr;
	}
	unique_ptr<ResultUnit> unit;
	{
		auto lock = handle->LockContext();
		handle->CheckExecutableInternal(*lock);
		unit = FetchUnitInternal(*lock);
	}
	if (!unit || unit->row_count == 0) {
		if (!HasError()) {
			handle->buffer->AssertNoBlockedSinks();
		}
		Close();
		return nullptr;
	}
	return unit;
}

const ResultFormatGlobalState &ResultStreamBase::FormatStateInternal() const {
	return handle->GetBufferedData().FormatState();
}

void ResultStreamBase::SetError(ErrorData error) {
	handle->SetError(std::move(error));
}

bool ResultStreamBase::HasError() const {
	return handle->HasError();
}

const string &ResultStreamBase::GetError() const {
	return handle->GetError();
}

const ErrorData &ResultStreamBase::GetErrorObject() const {
	return handle->GetErrorObject();
}

const ExceptionType &ResultStreamBase::GetErrorType() const {
	return handle->GetErrorType();
}

const vector<LogicalType> &ResultStreamBase::GetTypes() const {
	return handle->GetTypes();
}

const vector<Identifier> &ResultStreamBase::GetNames() const {
	return handle->GetNames();
}

const Identifier &ResultStreamBase::ColumnName(idx_t index) const {
	return handle->ColumnName(index);
}

idx_t ResultStreamBase::ColumnCount() const {
	return handle->ColumnCount();
}

StatementType ResultStreamBase::GetStatementType() const {
	return handle->GetStatementType();
}

const StatementProperties &ResultStreamBase::GetStatementProperties() const {
	return handle->GetStatementProperties();
}

const ClientProperties &ResultStreamBase::GetClientProperties() const {
	return handle->client_properties;
}

ClientProperties &ResultStreamBase::GetClientProperties() {
	return handle->client_properties;
}

} // namespace duckdb
