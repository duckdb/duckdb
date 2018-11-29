//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// main/result.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"

namespace duckdb {

//! The result object holds the result of a query. It can either hold an error
//! message or a DataChunk that represents the return value of the column.
class DuckDBResult {
  public:
	DuckDBResult();
	DuckDBResult(std::string error);

	bool GetSuccess() const {
		return success;
	}
	const std::string &GetErrorMessage() const {
		return error;
	}

	void Print();

	//! Returns the number of columns in the result
	size_t column_count() {
		return collection.types.size();
	}
	//! Returns the number of elements in the result
	size_t size() {
		return collection.count;
	}
	//! Returns a list of types of the result
	std::vector<TypeId> &types() {
		return collection.types;
	}

	//! Gets the value of the column at the given index. Note that this method
	//! is unsafe, it is up to the user to do (1) bounds checking (2) proper
	//! type checking. i.e. only use GetValue<int32_t> on columns of type
	//! TypeId::INTEGER
	template <class T> T GetValue(size_t column, size_t index) {
		auto &data = collection.GetChunk(index).data[column];
		auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
		return ((T *)data.data)[offset_in_chunk];
	}

	Value GetValue(size_t column, size_t index) {
		auto &data = collection.GetChunk(index).data[column];
		auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
		return data.GetValue(offset_in_chunk);
	}

	bool ValueIsNull(size_t column, size_t index) {
		auto &data = collection.GetChunk(index).data[column];
		auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
		return data.nullmask[offset_in_chunk];
	}

	//! The names of the result
	std::vector<std::string> names;
	ChunkCollection collection;

	bool success;
	std::string error;

  private:
	DuckDBResult(const DuckDBResult &) = delete;
};

} // namespace duckdb
