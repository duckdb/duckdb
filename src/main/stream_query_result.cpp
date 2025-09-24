#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

StreamQueryResult::StreamQueryResult(StatementType statement_type, StatementProperties properties,
                                     vector<LogicalType> types, vector<string> names,
                                     ClientProperties client_properties, shared_ptr<BufferedData> data)
    : QueryResult(QueryResultType::STREAM_RESULT, statement_type, std::move(properties), std::move(types),
                  std::move(names), std::move(client_properties)),
      buffered_data(std::move(data)) {
}

StreamQueryResult::StreamQueryResult(ErrorData error) : QueryResult(QueryResultType::STREAM_RESULT, std::move(error)) {
}

StreamQueryResult::~StreamQueryResult() {
}

string StreamQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[[STREAM RESULT]]";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

StreamQueryResult::LockContextResult StreamQueryResult::LockContext(shared_ptr<ClientContext> context) {
	LockContextResult result;
	result.context = context ? std::move(context) : buffered_data->GetContext();
	if (!result.context) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result.";
		error_str += "Hint: query results are closed upon closing the corresponding connection.";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
	result.lock = result.context->LockContext();
	return result;
}

StreamExecutionResult StreamQueryResult::ExecuteTaskInternal(LockContextResult &lock) {
	return buffered_data->ExecuteTaskInternal(*this, *lock.lock);
}

StreamExecutionResult StreamQueryResult::ExecuteTask() {
	auto lock = LockContext();
	return ExecuteTaskInternal(lock);
}

void StreamQueryResult::WaitForTask() {
	auto lock = LockContext();
	buffered_data->UnblockSinks();
	lock.context->WaitForTask(*lock.lock, *this);
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

unique_ptr<DataChunk> StreamQueryResult::FetchInternal(LockContextResult &lock) {
	bool invalidate_query = true;
	unique_ptr<DataChunk> chunk;
	try {
		// fetch the chunk and return it
		auto stream_execution_result = buffered_data->ReplenishBuffer(*this, *lock.lock);
		if (ExecutionErrorOccurred(stream_execution_result)) {
			return chunk;
		}
		chunk = buffered_data->Scan();
		if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
			lock.context->CleanupInternal(*lock.lock, this);
			chunk = nullptr;
		}
		return chunk;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (!Exception::InvalidatesTransaction(error.Type())) {
			// standard exceptions do not invalidate the current transaction
			invalidate_query = false;
		} else if (Exception::InvalidatesDatabase(error.Type())) {
			// fatal exceptions invalidate the entire database
			auto &config = lock.context->config;
			if (!config.query_verification_enabled) {
				auto &db_instance = DatabaseInstance::GetDatabase(*lock.context);
				ValidChecker::Invalidate(db_instance, error.RawMessage());
			}
		}
		lock.context->ProcessError(error, lock.context->GetCurrentQuery());
		SetError(std::move(error));
	} catch (...) { // LCOV_EXCL_START
		SetError(ErrorData("Unhandled exception in FetchInternal"));
	} // LCOV_EXCL_STOP
	lock.context->CleanupInternal(*lock.lock, this, invalidate_query);
	return nullptr;
}

unique_ptr<DataChunk> StreamQueryResult::FetchRaw() {
	unique_ptr<DataChunk> chunk;
	{
		auto lock = LockContext();
		CheckExecutableInternal(lock);
		chunk = FetchInternal(lock);
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
	if (HasError()) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	unique_ptr<ColumnDataCollection> collection;
	{
		auto lock = LockContext();
		collection = make_uniq<ColumnDataCollection>(*lock.context, types);
	}

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
	// Clear these so that the handles are destroyed before "context" goes out of scope
	append_state.current_chunk_state.handles.clear();

	shared_ptr<ManagedQueryResult> managed_result;
	{
		auto lock = LockContext();
		managed_result = QueryResultManager::Get(*lock.context).Add(std::move(collection));
	}
	auto result = make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(managed_result),
	                                                 client_properties);

	if (HasError()) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	return result;
}

bool StreamQueryResult::IsOpenInternal(LockContextResult &lock) {
	bool invalidated = !success || !lock.context;
	if (!invalidated) {
		invalidated = !lock.context->IsActiveResult(*lock.lock, *this);
	}
	return !invalidated;
}

void StreamQueryResult::CheckExecutableInternal(LockContextResult &lock) {
	if (!IsOpenInternal(lock)) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
}

bool StreamQueryResult::IsOpen() {
	auto context = buffered_data->GetContext();
	if (!success || !context) {
		return false;
	}
	auto lock = LockContext(std::move(context));
	return IsOpenInternal(lock);
}

void StreamQueryResult::Close() {
	buffered_data->Close();
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
