#include "duckdb/main/chunk_scan_state/query_result_stream.hpp"
#include "duckdb/main/query_result_stream.hpp"

namespace duckdb {

QueryResultStreamChunkScanState::QueryResultStreamChunkScanState(QueryResultStream &stream)
    : ChunkScanState(), stream(stream) {
}

QueryResultStreamChunkScanState::~QueryResultStreamChunkScanState() {
}

bool QueryResultStreamChunkScanState::LoadNextChunk(ErrorData &error_p) {
	if (finished) {
		current_chunk = nullptr;
		if (has_error) {
			error_p = error;
		}
		return !has_error;
	}
	try {
		current_chunk = stream.Fetch();
	} catch (std::exception &ex) {
		current_chunk = nullptr;
		finished = true;
		has_error = true;
		error = ErrorData(ex);
		error_p = error;
		return false;
	}
	offset = 0;
	if (!current_chunk) {
		finished = true;
		// The stream reports an execution error as an end of stream with the error recorded on it
		if (stream.HasError()) {
			has_error = true;
			error = stream.GetErrorObject();
			error_p = error;
			return false;
		}
	}
	return true;
}

bool QueryResultStreamChunkScanState::HasError() const {
	return has_error || stream.HasError();
}

ErrorData &QueryResultStreamChunkScanState::GetError() {
	if (!has_error) {
		has_error = true;
		error = stream.GetErrorObject();
	}
	return error;
}

const vector<LogicalType> &QueryResultStreamChunkScanState::Types() const {
	return stream.GetTypes();
}

const vector<Identifier> &QueryResultStreamChunkScanState::Names() const {
	return stream.GetNames();
}

} // namespace duckdb
