#include "duckdb/common/arrow_wrapper.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/common/result_arrow_wrapper.hpp"

#include "duckdb/main/query_result.hpp"
namespace duckdb {

ArrowSchemaWrapper::~ArrowSchemaWrapper() {
	if (arrow_schema.release) {
		for (int64_t child_idx = 0; child_idx < arrow_schema.n_children; child_idx++) {
			auto &child = *arrow_schema.children[child_idx];
			if (child.release) {
				child.release(&child);
			}
		}
		arrow_schema.release(&arrow_schema);
		arrow_schema.release = nullptr;
	}
}

ArrowArrayWrapper::~ArrowArrayWrapper() {
	if (arrow_array.release) {
		for (int64_t child_idx = 0; child_idx < arrow_array.n_children; child_idx++) {
			auto &child = *arrow_array.children[child_idx];
			if (child.release) {
				child.release(&child);
			}
		}
		arrow_array.release(&arrow_array);
		arrow_array.release = nullptr;
	}
}

ArrowArrayStreamWrapper::~ArrowArrayStreamWrapper() {
	if (arrow_array_stream.release) {
		arrow_array_stream.release(&arrow_array_stream);
		arrow_array_stream.release = nullptr;
	}
}

void ArrowArrayStreamWrapper::GetSchema(ArrowSchemaWrapper &schema) {
	D_ASSERT(arrow_array_stream.get_schema);
	// LCOV_EXCL_START
	if (arrow_array_stream.get_schema(&arrow_array_stream, &schema.arrow_schema)) {
		throw InvalidInputException("arrow_scan: get_schema failed(): %s", string(GetError()));
	}
	if (!schema.arrow_schema.release) {
		throw InvalidInputException("arrow_scan: released schema passed");
	}
	if (schema.arrow_schema.n_children < 1) {
		throw InvalidInputException("arrow_scan: empty schema passed");
	}
	// LCOV_EXCL_STOP
}

shared_ptr<ArrowArrayWrapper> ArrowArrayStreamWrapper::GetNextChunk() {
	auto current_chunk = make_shared<ArrowArrayWrapper>();
	if (arrow_array_stream.get_next(&arrow_array_stream, &current_chunk->arrow_array)) { // LCOV_EXCL_START
		throw InvalidInputException("arrow_scan: get_next failed(): %s", string(GetError()));
	} // LCOV_EXCL_STOP

	return current_chunk;
}

const char *ArrowArrayStreamWrapper::GetError() { // LCOV_EXCL_START
	return arrow_array_stream.get_last_error(&arrow_array_stream);
} // LCOV_EXCL_STOP

int ResultArrowArrayStreamWrapper::MyStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream->release) {
		return -1;
	}
	auto my_stream = (ResultArrowArrayStreamWrapper *)stream->private_data;
	if (!my_stream->column_types.empty()) {
		QueryResult::ToArrowSchema(out, my_stream->column_types, my_stream->column_names, my_stream->timezone_config);
		return 0;
	}

	auto &result = *my_stream->result;
	if (!result.success) {
		my_stream->last_error = "Query Failed";
		return -1;
	}
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = (StreamQueryResult &)result;
		if (!stream_result.IsOpen()) {
			my_stream->last_error = "Query Stream is closed";
			return -1;
		}
	}
	if (my_stream->column_types.empty()) {
		my_stream->column_types = result.types;
		my_stream->column_names = result.names;
	}
	QueryResult::ToArrowSchema(out, my_stream->column_types, my_stream->column_names, my_stream->timezone_config);
	return 0;
}

int ResultArrowArrayStreamWrapper::MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->release) {
		return -1;
	}
	auto my_stream = (ResultArrowArrayStreamWrapper *)stream->private_data;
	auto &result = *my_stream->result;
	if (!result.success) {
		my_stream->last_error = "Query Failed";
		return -1;
	}
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = (StreamQueryResult &)result;
		if (!stream_result.IsOpen()) {
			// Nothing to output
			out->release = nullptr;
			return 0;
		}
	}
	if (my_stream->column_types.empty()) {
		my_stream->column_types = result.types;
		my_stream->column_names = result.names;
	}
	unique_ptr<DataChunk> chunk_result = result.Fetch();
	if (!chunk_result) {
		// Nothing to output
		out->release = nullptr;
		return 0;
	}
	unique_ptr<DataChunk> agg_chunk_result = make_unique<DataChunk>();
	agg_chunk_result->Initialize(chunk_result->GetTypes());
	agg_chunk_result->Append(*chunk_result, true);

	while (agg_chunk_result->size() < my_stream->batch_size) {
		auto new_chunk = result.Fetch();
		if (!new_chunk) {
			break;
		} else {
			agg_chunk_result->Append(*new_chunk, true);
		}
	}
	agg_chunk_result->ToArrowArray(out);
	return 0;
}

void ResultArrowArrayStreamWrapper::MyStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream->release) {
		return;
	}
	stream->release = nullptr;
	delete (ResultArrowArrayStreamWrapper *)stream->private_data;
}

const char *ResultArrowArrayStreamWrapper::MyStreamGetLastError(struct ArrowArrayStream *stream) {
	if (!stream->release) {
		return "stream was released";
	}
	D_ASSERT(stream->private_data);
	auto my_stream = (ResultArrowArrayStreamWrapper *)stream->private_data;
	return my_stream->last_error.c_str();
}
ResultArrowArrayStreamWrapper::ResultArrowArrayStreamWrapper(unique_ptr<QueryResult> result_p, idx_t batch_size_p)
    : result(move(result_p)) {
	//! We first initialize the private data of the stream
	stream.private_data = this;
	//! Ceil Approx_Batch_Size/STANDARD_VECTOR_SIZE
	if (batch_size_p == 0) {
		throw std::runtime_error("Approximate Batch Size of Record Batch MUST be higher than 0");
	}
	batch_size = batch_size_p;
	//! We initialize the stream functions
	stream.get_schema = ResultArrowArrayStreamWrapper::MyStreamGetSchema;
	stream.get_next = ResultArrowArrayStreamWrapper::MyStreamGetNext;
	stream.release = ResultArrowArrayStreamWrapper::MyStreamRelease;
	stream.get_last_error = ResultArrowArrayStreamWrapper::MyStreamGetLastError;
}

unique_ptr<DataChunk> ArrowUtil::FetchNext(QueryResult &result) {
	auto chunk = result.Fetch();
	if (!result.success) {
		throw std::runtime_error(result.error);
	}
	return chunk;
}

unique_ptr<DataChunk> ArrowUtil::FetchChunk(QueryResult *result, idx_t chunk_size) {

	auto data_chunk = FetchNext(*result);
	if (!data_chunk) {
		return data_chunk;
	}
	while (data_chunk->size() < chunk_size) {
		auto next_chunk = FetchNext(*result);
		if (!next_chunk || next_chunk->size() == 0) {
			break;
		}
		data_chunk->Append(*next_chunk, true);
	}
	return data_chunk;
}

} // namespace duckdb