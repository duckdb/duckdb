#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"

namespace duckdb {

ArrowSchemaWrapper::~ArrowSchemaWrapper() {
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
		D_ASSERT(!arrow_schema.release);
	}
}

ArrowArrayWrapper::~ArrowArrayWrapper() {
	if (arrow_array.release) {
		arrow_array.release(&arrow_array);
		D_ASSERT(!arrow_array.release);
	}
}

ArrowArrayStreamWrapper::~ArrowArrayStreamWrapper() {
	if (arrow_array_stream.release) {
		arrow_array_stream.release(&arrow_array_stream);
		D_ASSERT(!arrow_array_stream.release);
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
	auto current_chunk = make_shared_ptr<ArrowArrayWrapper>();
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
	out->release = nullptr;
	auto my_stream = reinterpret_cast<ResultArrowArrayStreamWrapper *>(stream->private_data);
	if (!my_stream->column_types.empty()) {
		try {
			ArrowConverter::ToArrowSchema(out, my_stream->column_types, my_stream->column_names,
			                              my_stream->result->client_properties);
		} catch (std::runtime_error &e) {
			my_stream->last_error = ErrorData(e);
			return -1;
		}
		return 0;
	}

	auto &result = *my_stream->result;
	if (result.HasError()) {
		my_stream->last_error = result.GetErrorObject();
		return -1;
	}
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = result.Cast<StreamQueryResult>();
		if (!stream_result.IsOpen()) {
			my_stream->last_error = ErrorData("Query Stream is closed");
			return -1;
		}
	}
	if (my_stream->column_types.empty()) {
		my_stream->column_types = result.types;
		my_stream->column_names = result.names;
	}
	try {
		ArrowConverter::ToArrowSchema(out, my_stream->column_types, my_stream->column_names,
		                              my_stream->result->client_properties);
	} catch (std::runtime_error &e) {
		my_stream->last_error = ErrorData(e);
		return -1;
	}
	return 0;
}

int ResultArrowArrayStreamWrapper::MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->release) {
		return -1;
	}
	auto my_stream = reinterpret_cast<ResultArrowArrayStreamWrapper *>(stream->private_data);
	auto &result = *my_stream->result;
	auto &scan_state = *my_stream->scan_state;
	if (result.HasError()) {
		my_stream->last_error = result.GetErrorObject();
		return -1;
	}
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = result.Cast<StreamQueryResult>();
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
	idx_t result_count;
	ErrorData error;
	if (!ArrowUtil::TryFetchChunk(scan_state, result.client_properties, my_stream->batch_size, out, result_count,
	                              error)) {
		D_ASSERT(error.HasError());
		my_stream->last_error = error;
		return -1;
	}
	if (result_count == 0) {
		// Nothing to output
		out->release = nullptr;
	}
	return 0;
}

void ResultArrowArrayStreamWrapper::MyStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	stream->release = nullptr;
	delete reinterpret_cast<ResultArrowArrayStreamWrapper *>(stream->private_data);
}

const char *ResultArrowArrayStreamWrapper::MyStreamGetLastError(struct ArrowArrayStream *stream) {
	if (!stream->release) {
		return "stream was released";
	}
	D_ASSERT(stream->private_data);
	auto my_stream = reinterpret_cast<ResultArrowArrayStreamWrapper *>(stream->private_data);
	return my_stream->last_error.Message().c_str();
}

ResultArrowArrayStreamWrapper::ResultArrowArrayStreamWrapper(unique_ptr<QueryResult> result_p, idx_t batch_size_p)
    : result(std::move(result_p)), scan_state(make_uniq<QueryResultChunkScanState>(*result)) {
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

bool ArrowUtil::TryFetchChunk(ChunkScanState &scan_state, ClientProperties options, idx_t batch_size, ArrowArray *out,
                              idx_t &count, ErrorData &error) {
	count = 0;
	ArrowAppender appender(scan_state.Types(), batch_size, std::move(options));
	auto remaining_tuples_in_chunk = scan_state.RemainingInChunk();
	if (remaining_tuples_in_chunk) {
		// We start by scanning the non-finished current chunk
		idx_t cur_consumption = MinValue(remaining_tuples_in_chunk, batch_size);
		count += cur_consumption;
		auto &current_chunk = scan_state.CurrentChunk();
		appender.Append(current_chunk, scan_state.CurrentOffset(), scan_state.CurrentOffset() + cur_consumption,
		                current_chunk.size());
		scan_state.IncreaseOffset(cur_consumption);
	}
	while (count < batch_size) {
		if (!scan_state.LoadNextChunk(error)) {
			if (scan_state.HasError()) {
				error = scan_state.GetError();
			}
			return false;
		}
		if (scan_state.ChunkIsEmpty()) {
			// The scan was successful, but an empty chunk was returned
			break;
		}
		auto &current_chunk = scan_state.CurrentChunk();
		if (scan_state.Finished() || current_chunk.size() == 0) {
			break;
		}
		// The amount we still need to append into this chunk
		auto remaining = batch_size - count;

		// The amount remaining, capped by the amount left in the current chunk
		auto to_append_to_batch = MinValue(remaining, scan_state.RemainingInChunk());
		appender.Append(current_chunk, 0, to_append_to_batch, current_chunk.size());
		count += to_append_to_batch;
		scan_state.IncreaseOffset(to_append_to_batch);
	}
	if (count > 0) {
		*out = appender.Finalize();
	}
	return true;
}

idx_t ArrowUtil::FetchChunk(ChunkScanState &scan_state, ClientProperties options, idx_t chunk_size, ArrowArray *out) {
	ErrorData error;
	idx_t result_count;
	if (!TryFetchChunk(scan_state, std::move(options), chunk_size, out, result_count, error)) {
		error.Throw();
	}
	return result_count;
}

} // namespace duckdb
