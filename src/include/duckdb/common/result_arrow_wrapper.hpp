//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/result_arrow_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"
#include "duckdb/common/arrow_wrapper.hpp"

namespace duckdb {
class ResultArrowArrayStreamWrapper {
public:
	explicit ResultArrowArrayStreamWrapper(unique_ptr<QueryResult> result, idx_t approx_batch_size);
	ArrowArrayStream stream;
	unique_ptr<QueryResult> result;
	std::string last_error;
	idx_t vectors_per_chunk;

private:
	static int MyStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out);
	static int MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out);
	static void MyStreamRelease(struct ArrowArrayStream *stream);
	static const char *MyStreamGetLastError(struct ArrowArrayStream *stream);
};
} // namespace duckdb