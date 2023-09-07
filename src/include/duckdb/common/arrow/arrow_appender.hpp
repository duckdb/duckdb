//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_appender.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow.hpp"

namespace duckdb {

struct ArrowAppendData;

//! The ArrowAppender class can be used to incrementally construct an arrow array by appending data chunks into it
class ArrowAppender {
public:
	DUCKDB_API ArrowAppender(vector<LogicalType> types, idx_t initial_capacity, ClientProperties options);
	DUCKDB_API ~ArrowAppender();

	//! Append a data chunk to the underlying arrow array
	DUCKDB_API void Append(DataChunk &input, idx_t from, idx_t to, idx_t input_size);
	//! Returns the underlying arrow array
	DUCKDB_API ArrowArray Finalize();

public:
	static void ReleaseArray(ArrowArray *array);
	static ArrowArray *FinalizeChild(const LogicalType &type, ArrowAppendData &append_data);
	static unique_ptr<ArrowAppendData> InitializeChild(const LogicalType &type, idx_t capacity,
	                                                   ClientProperties &options);

private:
	//! The types of the chunks that will be appended in
	vector<LogicalType> types;
	//! The root arrow append data
	vector<unique_ptr<ArrowAppendData>> root_data;
	//! The total row count that has been appended
	idx_t row_count = 0;

	ClientProperties options;
};

} // namespace duckdb
