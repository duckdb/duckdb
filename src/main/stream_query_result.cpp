#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

StreamQueryResult::StreamQueryResult(StatementType statement_type, StatementProperties properties,
                                     vector<LogicalType> types, vector<Identifier> names,
                                     ClientProperties client_properties, shared_ptr<BufferedData> data)
    : QueryResult(QueryResultType::STREAM_RESULT, statement_type, std::move(properties), std::move(types),
                  std::move(names), std::move(client_properties)),
      buffered_data(std::move(data)) {
	context = buffered_data->GetContext();
	// A notify callback passed at execution time already armed a notifier on the buffer
	result_notifier = buffered_data->GetResultNotifier();
}

StreamQueryResult::StreamQueryResult(ErrorData error) : QueryResult(QueryResultType::STREAM_RESULT, std::move(error)) {
}

StreamQueryResult::~StreamQueryResult() {
	if (result_notifier) {
		result_notifier->Clear();
	}
}

string StreamQueryResult::ToString() {
	string result;
	if (!HasError()) {
		result = HeaderToString();
		result += "[[STREAM RESULT]]";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

unique_ptr<ClientContextLock> StreamQueryResult::LockContext() {
	if (!context) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
	return context->LockContext();
}

StreamExecutionResult StreamQueryResult::ExecuteTaskInternal(ClientContextLock &lock) {
	return buffered_data->ExecuteTaskInternal(*this, lock);
}

StreamExecutionResult StreamQueryResult::ExecuteTask() {
	auto lock = LockContext();
	return ExecuteTaskInternal(*lock);
}

void StreamQueryResult::HandleFetchFailure(ClientContextLock &lock, ErrorData error) {
	bool invalidate_query = true;
	if (!context->ErrorInvalidatesTransaction(error.Type())) {
		// standard exceptions do not invalidate the current transaction
		invalidate_query = false;
	} else if (Exception::InvalidatesDatabase(error.Type())) {
		// fatal exceptions invalidate the entire database
		auto &db_instance = DatabaseInstance::GetDatabase(*context);
		ValidChecker::Invalidate(db_instance, error.RawMessage());
	}
	context->ProcessError(error, context->GetCurrentQuery());
	SetError(std::move(error));
	context->CleanupInternal(lock, this, invalidate_query);
}

StreamExecutionResult StreamQueryResult::TryFetchChunk(unique_ptr<DataChunk> &out_chunk) {
	out_chunk.reset();
	if (!context) {
		// The stream already closed: keep reporting the terminal state
		return HasError() ? StreamExecutionResult::EXECUTION_ERROR : StreamExecutionResult::EXECUTION_FINISHED;
	}
	if (!buffered_data || !buffered_data->IsAsync()) {
		throw InvalidInputException(
		    "TryFetchChunk can only be used on a streaming result executed with QueryResultExecutionMode::ASYNC");
	}
	StreamExecutionResult execution_result;
	{
		auto lock = LockContext();
		CheckExecutableInternal(*lock);
		try {
			execution_result = ExecuteTaskInternal(*lock);
			if (execution_result == StreamExecutionResult::CHUNK_READY ||
			    execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
				out_chunk = buffered_data->Scan();
				if (out_chunk && out_chunk->ColumnCount() != 0 && out_chunk->size() != 0) {
					return StreamExecutionResult::CHUNK_READY;
				}
				// The buffer is drained and execution is done: end of stream
				out_chunk.reset();
				context->CleanupInternal(*lock, this);
				// Cleanup can fail (autocommit commit): it sets the error without throwing
				execution_result =
				    HasError() ? StreamExecutionResult::EXECUTION_ERROR : StreamExecutionResult::EXECUTION_FINISHED;
			}
		} catch (std::exception &ex) {
			HandleFetchFailure(*lock, ErrorData(ex));
			execution_result = StreamExecutionResult::EXECUTION_ERROR;
		} catch (...) { // LCOV_EXCL_START
			SetError(ErrorData("Unhandled exception in TryFetchChunk"));
			context->CleanupInternal(*lock, this, true);
			execution_result = StreamExecutionResult::EXECUTION_ERROR;
		} // LCOV_EXCL_STOP
	}
	if (execution_result == StreamExecutionResult::EXECUTION_FINISHED ||
	    execution_result == StreamExecutionResult::EXECUTION_ERROR ||
	    execution_result == StreamExecutionResult::EXECUTION_CANCELLED) {
		Close();
	}
	return execution_result;
}

void StreamQueryResult::WaitForTask() {
	auto lock = LockContext();
	buffered_data->UnblockSinks();
	context->WaitForTask(*lock, *this);
}

static bool ExecutionErrorOccurred(StreamExecutionResult result) {
	if (result == StreamExecutionResult::EXECUTION_CANCELLED) {
		return true;
	}
	if (result == StreamExecutionResult::EXECUTION_ERROR) {
		return true;
	}
	return false;
}

unique_ptr<DataChunk> StreamQueryResult::FetchNextInternal(ClientContextLock &lock) {
	unique_ptr<DataChunk> chunk;
	try {
		// fetch the chunk and return it
		auto stream_execution_result = buffered_data->ReplenishBuffer(*this, lock);
		if (ExecutionErrorOccurred(stream_execution_result)) {
			return chunk;
		}
		chunk = buffered_data->Scan();
		if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
			context->CleanupInternal(lock, this);
			chunk = nullptr;
		}
		return chunk;
	} catch (std::exception &ex) {
		HandleFetchFailure(lock, ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		SetError(ErrorData("Unhandled exception in FetchInternal"));
		context->CleanupInternal(lock, this, true);
	} // LCOV_EXCL_STOP
	return nullptr;
}

unique_ptr<DataChunk> StreamQueryResult::FetchInternal() {
	unique_ptr<DataChunk> chunk;
	{
		auto lock = LockContext();
		CheckExecutableInternal(*lock);
		chunk = FetchNextInternal(*lock);
	}
	if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
		Close();
		return nullptr;
	}
	return chunk;
}

#ifdef DUCKDB_ALTERNATIVE_VERIFY
static unique_ptr<DataChunk> AlternativeFetch(StreamQueryResult &stream_result) {
	// We first use StreamQueryResult::ExecuteTask until IsChunkReady becomes true
	// then call Fetch
	StreamExecutionResult execution_result;
	while (!StreamQueryResult::IsChunkReady(execution_result = stream_result.ExecuteTask())) {
		if (execution_result == StreamExecutionResult::BLOCKED) {
			stream_result.WaitForTask();
		}
	}
	if (execution_result == StreamExecutionResult::EXECUTION_CANCELLED) {
		throw InvalidInputException("The execution of the query was cancelled before it could finish, likely "
		                            "caused by executing a different query");
	}
	if (execution_result == StreamExecutionResult::EXECUTION_ERROR) {
		stream_result.ThrowError();
	}
	return stream_result.Fetch();
}
#endif

unique_ptr<MaterializedQueryResult> StreamQueryResult::Materialize() {
	if (HasError() || !context) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), GetTypes());

	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	while (true) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
		auto chunk = AlternativeFetch(*this);
#else
		auto chunk = Fetch();
#endif
		if (!chunk || chunk->size() == 0) {
			break;
		}
		collection->Append(append_state, *chunk);
	}
	auto result = make_uniq<MaterializedQueryResult>(GetStatementType(), GetStatementProperties(), GetNames(),
	                                                 std::move(collection), client_properties);
	if (HasError()) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	return result;
}

bool StreamQueryResult::IsOpenInternal(ClientContextLock &lock) {
	bool invalidated = HasError() || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, *this);
	}
	return !invalidated;
}

void StreamQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	if (!IsOpenInternal(lock)) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
}

bool StreamQueryResult::IsOpen() {
	if (HasError() || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

void StreamQueryResult::Close() {
	if (result_notifier) {
		result_notifier->Clear();
	}
	buffered_data->Close();
	if (context) {
		auto lock = LockContext();
		if (context->IsActiveResult(*lock, *this)) {
			// Abandoned before the stream was fully drained: release the active-query state now
			// (matching InitialCleanup) instead of leaking it until the next query or context teardown.
			context->CleanupInternal(*lock, this, false);
		}
	}
	context.reset();
}

bool StreamQueryResult::IsChunkReady(StreamExecutionResult result) {
	if (result == StreamExecutionResult::CHUNK_READY) {
		// A chunk is ready to be fetched with Fetch()
		return true;
	}
	if (result == StreamExecutionResult::EXECUTION_CANCELLED) {
		// Another query execution was started that cancelled this one
		return true;
	}
	if (result == StreamExecutionResult::EXECUTION_ERROR) {
		// An error was encountered while executing the final pipeline
		return true;
	}
	if (result == StreamExecutionResult::EXECUTION_FINISHED) {
		// The final pipeline completed successfully
		return true;
	}
	return false;
}

} // namespace duckdb
