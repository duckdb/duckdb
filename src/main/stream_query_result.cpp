#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

StreamQueryResult::StreamQueryResult(StatementType statement_type, StatementProperties properties,
                                     vector<LogicalType> types, vector<string> names,
                                     ClientProperties client_properties, shared_ptr<BufferedData> buffered_data)
    : QueryResult(QueryResultType::STREAM_RESULT, statement_type, std::move(properties), std::move(types),
                  std::move(names), std::move(client_properties)),
      buffered_data(buffered_data) {
	context = buffered_data->GetContext();
}

StreamQueryResult::~StreamQueryResult() {
}

string StreamQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[[BUFFERED RESULT]]";
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

unique_ptr<DataChunk> StreamQueryResult::FetchRaw() {
	bool last_chunk = false;
	unique_ptr<DataChunk> chunk;
	{
		auto lock = LockContext();
		CheckExecutableInternal(*lock);
		buffered_data->ReplenishBuffer(*this, *lock);
		chunk = buffered_data->Scan();
		if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
			context->CleanupInternal(*lock, this);
			chunk = nullptr;
			last_chunk = true;
		}
	}
	if (last_chunk) {
		// This will grab the ClientContextLock so it's done outside of the block above
		Close();
	}
	return chunk;
}

unique_ptr<MaterializedQueryResult> StreamQueryResult::Materialize() {
	if (HasError() || !context) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);

	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	while (true) {
		auto chunk = Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		collection->Append(append_state, *chunk);
	}
	auto result =
	    make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(collection), client_properties);
	if (HasError()) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	return result;
}

bool StreamQueryResult::IsOpenInternal(ClientContextLock &lock) {
	bool invalidated = !success || !context;
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
	if (!success || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

void StreamQueryResult::Close() {
	buffered_data->Close();
	context.reset();
}

} // namespace duckdb
