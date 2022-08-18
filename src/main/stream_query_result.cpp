#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

StreamQueryResult::StreamQueryResult(StatementType statement_type, StatementProperties properties,
                                     shared_ptr<ClientContext> context_p, vector<LogicalType> types,
                                     vector<string> names)
    : QueryResult(QueryResultType::STREAM_RESULT, statement_type, properties, move(types), move(names),
                  context_p->GetClientProperties()),
      context(move(context_p)) {
	D_ASSERT(context);
}

StreamQueryResult::~StreamQueryResult() {
}

string StreamQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[[STREAM RESULT]]";
	} else {
		result = error + "\n";
	}
	return result;
}

unique_ptr<ClientContextLock> StreamQueryResult::LockContext() {
	if (!context) {
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result\nError: %s",
		                            error);
	}
	return context->LockContext();
}

void StreamQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	if (!IsOpenInternal(lock)) {
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result\nError: %s",
		                            error);
	}
}

unique_ptr<DataChunk> StreamQueryResult::FetchRaw() {
	unique_ptr<DataChunk> chunk;
	{
		auto lock = LockContext();
		CheckExecutableInternal(*lock);
		chunk = context->Fetch(*lock, *this);
	}
	if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
		Close();
		return nullptr;
	}
	return chunk;
}

unique_ptr<MaterializedQueryResult> StreamQueryResult::Materialize() {
	if (!success || !context) {
		return make_unique<MaterializedQueryResult>(error);
	}
	auto collection = make_unique<ColumnDataCollection>(Allocator::DefaultAllocator(), types);

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
	    make_unique<MaterializedQueryResult>(statement_type, properties, names, move(collection), client_properties);
	if (!success) {
		return make_unique<MaterializedQueryResult>(error);
	}
	return result;
}

bool StreamQueryResult::IsOpenInternal(ClientContextLock &lock) {
	bool invalidated = !success || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, this);
	}
	return !invalidated;
}

bool StreamQueryResult::IsOpen() {
	if (!success || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

void StreamQueryResult::Close() {
	context.reset();
}

} // namespace duckdb
