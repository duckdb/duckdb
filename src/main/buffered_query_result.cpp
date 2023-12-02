#include "duckdb/main/buffered_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

BufferedQueryResult::BufferedQueryResult(StatementType statement_type, StatementProperties properties,
                                         vector<LogicalType> types, vector<string> names,
                                         ClientProperties client_properties, shared_ptr<BufferedData> buffered_data)
    : QueryResult(QueryResultType::BUFFERED_RESULT, statement_type, std::move(properties), std::move(types),
                  std::move(names), std::move(client_properties)),
      buffered_data(buffered_data) {
	scan_state.chunk = nullptr;
	scan_state.offset = 0;
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
	buffered_data->ReplenishBuffer(*this);

	unique_ptr<DataChunk> chunk;
	idx_t remaining = STANDARD_VECTOR_SIZE;
	while (remaining > 0) {
		if (!scan_state.chunk) {
			scan_state.chunk = buffered_data->Scan();
			if (!scan_state.chunk || scan_state.chunk->size() == 0) {
				// Nothing left to scan
				scan_state.chunk.reset();
				break;
			}
			scan_state.offset = 0;
		}
		auto chunk_size = scan_state.chunk->size();
		D_ASSERT(chunk_size > scan_state.offset);
		// How many tuples are still left in our scanned chunk
		auto left_in_chunk = chunk_size - scan_state.offset;
		// How many we can append to the chunk we're currently creating
		auto to_append = MinValue(left_in_chunk, remaining);

		// First make sure we have a result chunk
		if (!chunk) {
			if (!scan_state.offset) {
				// No tuples have been scanned from this yet, just take it
				chunk = std::move(scan_state.chunk);
				scan_state.chunk.reset();
			} else {
				chunk = make_uniq<DataChunk>();
				chunk->Initialize(Allocator::DefaultAllocator(), scan_state.chunk->GetTypes(), STANDARD_VECTOR_SIZE);
				chunk->SetCardinality(0);
			}
		}

		if (scan_state.chunk) {
			// We have not moved the chunk, need to scan from it
			for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
				D_ASSERT(scan_state.chunk->data[i].GetVectorType() == VectorType::FLAT_VECTOR);
				VectorOperations::CopyPartial(scan_state.chunk->data[i], chunk->data[i], scan_state.chunk->size(),
				                              scan_state.offset, to_append, chunk->size());
			}
			chunk->SetCardinality(chunk->size() + to_append);
		}

		// Reset the scan state if necessary
		scan_state.offset += to_append;
		if (scan_state.offset >= chunk_size) {
			scan_state.offset = 0;
			scan_state.chunk.reset();
		}
		remaining -= to_append;
	}
	if (chunk) {
		chunk->SetCardinality(STANDARD_VECTOR_SIZE - remaining);
	}
	return chunk;
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
