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

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return error; }

	void Print();

	size_t column_count() { return collection.types.size(); }
	size_t size() { return collection.count; }
	std::vector<TypeId> &types() { return collection.types; }

	std::vector<std::string> names;
	ChunkCollection collection;

	bool success;
	std::string error;

  private:
	DuckDBResult(const DuckDBResult &) = delete;
};

} // namespace duckdb
