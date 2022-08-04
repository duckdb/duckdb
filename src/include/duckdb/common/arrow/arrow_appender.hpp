//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_appender.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow_converter.hpp"

struct ArrowSchema;

namespace duckdb {

struct ArrowAppendData;

//! The ArrowAppender class can be used to incrementally construct an arrow array by appending data chunks into it
class ArrowAppender {
public:
	DUCKDB_API ArrowAppender(vector<LogicalType> types, idx_t initial_capacity);
	DUCKDB_API ~ArrowAppender();

	//! Append a data chunk to the underlying arrow array
	DUCKDB_API void Append(DataChunk &input);
	//! Returns the underlying arrow array
	DUCKDB_API ArrowArray Finalize();

private:
	//! The types of the chunks that will be appended in
	vector<LogicalType> types;
	//! The root arrow append data
	vector<unique_ptr<ArrowAppendData>> root_data;
	//! The total row count that has been appended
	idx_t row_count = 0;
};

} // namespace duckdb
