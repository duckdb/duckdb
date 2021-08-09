#include "duckdb/common/arrow_wrapper.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/main/stream_query_result.hpp"
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

unique_ptr<ArrowArrayWrapper> ArrowArrayStreamWrapper::GetNextChunk() {
	auto current_chunk = make_unique<ArrowArrayWrapper>();
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
	auto &result = *my_stream->result;
	if (!result.success) {
		my_stream->last_error = "Query Failed";
		return -1;
	}
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = (StreamQueryResult &)result;
		if (!stream_result.is_open) {
			my_stream->last_error = "Query Stream is closed";
			return -1;
		}
	}
	result.ToArrowSchema(out);
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
		if (!stream_result.is_open) {
			my_stream->last_error = "Query Stream is closed";
			return -1;
		}
	}
	auto data_chunk = result.Fetch();
	if (!data_chunk) {
		//! Nothing to output
		out->release = nullptr;
		return 0;
	}
	data_chunk->ToArrowArray(out);
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
ResultArrowArrayStreamWrapper::ResultArrowArrayStreamWrapper(unique_ptr<QueryResult> result_p)
    : result(move(result_p)) {
	//! We first initialize the private data of the stream
	stream.private_data = this;

	//! We initialize the stream functions
	stream.get_schema = ResultArrowArrayStreamWrapper::MyStreamGetSchema;
	stream.get_next = ResultArrowArrayStreamWrapper::MyStreamGetNext;
	stream.release = ResultArrowArrayStreamWrapper::MyStreamRelease;
	stream.get_last_error = ResultArrowArrayStreamWrapper::MyStreamGetLastError;
}

} // namespace duckdb