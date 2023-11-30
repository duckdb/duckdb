#include "duckdb/main/buffered_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

void BufferedData::AddToBacklog(InterruptState state) {
	lock_guard<mutex> lock(glock);
	blocked_sinks.push(state);
}

bool BufferedData::BufferIsFull() const {
	return buffered_chunks_count >= BUFFER_SIZE;
}

void BufferedData::ReplenishBuffer(BufferedQueryResult &result) {
	if (!context) {
		// Result has already been closed
		return;
	}
	unique_lock<mutex> lock(glock);
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return;
	}
	// Reschedule all the blocked sinks
	while (!blocked_sinks.empty()) {
		auto &state = blocked_sinks.front();
		state.Callback();
		blocked_sinks.pop();
	}
	// We have to release the lock so the ResultCollector can fill the buffer
	lock.unlock();
	// Let the executor run until the buffer is no longer empty
	auto context_lock = context->LockContext();
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(*context_lock, result))) {
		if (buffered_chunks_count >= BUFFER_SIZE) {
			break;
		}
	}
}

unique_ptr<DataChunk> BufferedData::Fetch(BufferedQueryResult &result) {
	ReplenishBuffer(result);

	unique_lock<mutex> lock(glock);
	if (!context || buffered_chunks.empty()) {
		context.reset();
		return nullptr;
	}

	// Take a chunk from the queue
	auto chunk = std::move(buffered_chunks.front());
	buffered_chunks.pop();
	auto count = buffered_chunks_count.load();
	Printer::Print(StringUtil::Format("Buffer capacity: %d", count));
	buffered_chunks_count--;
	return chunk;
}

void BufferedData::Populate(unique_ptr<DataChunk> chunk) {
	unique_lock<mutex> lock(glock);
	buffered_chunks.push(std::move(chunk));
	buffered_chunks_count++;
}

// --------- BUFFERED QUERY RESULT ----------

BufferedQueryResult::BufferedQueryResult(StatementType statement_type, StatementProperties properties,
                                         vector<LogicalType> types, vector<string> names,
                                         ClientProperties client_properties, shared_ptr<BufferedData> buffered_data)
    : QueryResult(QueryResultType::BUFFERED_RESULT, statement_type, std::move(properties), std::move(types),
                  std::move(names), std::move(client_properties)),
      buffered_data(buffered_data) {
}

BufferedQueryResult::~BufferedQueryResult() {
}

string BufferedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[[BUFFERED RESULT]]";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

unique_ptr<ClientContextLock> BufferedQueryResult::LockContext() {
	if (!context) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
	return context->LockContext();
}

unique_ptr<DataChunk> BufferedQueryResult::FetchRaw() {
	return buffered_data->Fetch(*this);
}

unique_ptr<MaterializedQueryResult> BufferedQueryResult::Materialize() {
	// if (HasError() || !context) {
	//	return make_uniq<MaterializedQueryResult>(GetErrorObject());
	//}
	// auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);

	// ColumnDataAppendState append_state;
	// collection->InitializeAppend(append_state);
	// while (true) {
	//	auto chunk = Fetch();
	//	if (!chunk || chunk->size() == 0) {
	//		break;
	//	}
	//	collection->Append(append_state, *chunk);
	//}
	// auto result =
	//    make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(collection),
	//    client_properties);
	// if (HasError()) {
	//	return make_uniq<MaterializedQueryResult>(GetErrorObject());
	//}
	// return result;
	return nullptr;
}

bool BufferedQueryResult::IsOpenInternal(ClientContextLock &lock) {
	bool invalidated = !success || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, *this);
	}
	return !invalidated;
}

void BufferedQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	if (!IsOpenInternal(lock)) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
}

bool BufferedQueryResult::IsOpen() {
	if (!success || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

void BufferedQueryResult::Close() {
	context.reset();
}

} // namespace duckdb
