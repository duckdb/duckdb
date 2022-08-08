//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/helper.hpp"

//! Here we have the internal duckdb classes that interact with Arrow's Internal Header (i.e., duckdb/commons/arrow.hpp)
namespace duckdb {
class QueryResult;
class DataChunk;

class ArrowSchemaWrapper {
public:
	ArrowSchema arrow_schema;

	ArrowSchemaWrapper() {
		arrow_schema.release = nullptr;
	}

	~ArrowSchemaWrapper();
};
class ArrowArrayWrapper {
public:
	ArrowArray arrow_array;
	ArrowArrayWrapper() {
		arrow_array.length = 0;
		arrow_array.release = nullptr;
	}
	~ArrowArrayWrapper();
};

class ArrowArrayStreamWrapper {
public:
	ArrowArrayStream arrow_array_stream;
	int64_t number_of_rows;

public:
	void GetSchema(ArrowSchemaWrapper &schema);

	shared_ptr<ArrowArrayWrapper> GetNextChunk();

	const char *GetError();

	~ArrowArrayStreamWrapper();
	ArrowArrayStreamWrapper() {
		arrow_array_stream.release = nullptr;
	}
};

class ArrowUtil {
public:
	static bool TryFetchChunk(QueryResult *result, idx_t chunk_size, ArrowArray *out, idx_t &result_count,
	                          string &error);
	static idx_t FetchChunk(QueryResult *result, idx_t chunk_size, ArrowArray *out);

private:
	static bool TryFetchNext(QueryResult &result, unique_ptr<DataChunk> &out, string &error);
};
} // namespace duckdb
