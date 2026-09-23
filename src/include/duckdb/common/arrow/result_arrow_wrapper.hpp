//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/result_arrow_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {
//! Exports a query result as an Arrow C stream. A result that can be drained is streamed one batch at a time;
//! a result that cannot (a statement that completes before its result is returned, a detached result, or a handle
//! whose retention is already retained) is completed on the first pull and read from its collection.
class ResultArrowArrayStreamWrapper {
public:
	explicit ResultArrowArrayStreamWrapper(unique_ptr<QueryResult> result, idx_t batch_size);

public:
	ArrowArrayStream stream;
	ClientProperties client_properties;
	//! The handle, when the result is read from its retained collection
	unique_ptr<QueryResult> result;
	//! The engine stream, when the result is drained
	unique_ptr<QueryResultStream> stream_result;
	ErrorData last_error;
	idx_t batch_size;
	vector<LogicalType> column_types;
	vector<string> column_names;
	unique_ptr<ChunkScanState> scan_state;
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;

private:
	static int MyStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out);
	static int MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out);
	static void MyStreamRelease(struct ArrowArrayStream *stream);
	static const char *MyStreamGetLastError(struct ArrowArrayStream *stream);
};
} // namespace duckdb
