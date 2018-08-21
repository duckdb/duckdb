//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "catalog/catalog.hpp"
#include "common/types/chunk_collection.hpp"

namespace duckdb {
class DuckDB;
class DuckDBConnection;
class DuckDBResult;

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDB {
  public:
	DuckDB(const char *path);

	Catalog catalog;
};

//! A connection to a database. This represents a (client) connection that can
//! be used to query the database.
class DuckDBConnection {
  public:
	DuckDBConnection(DuckDB &database);

	std::unique_ptr<DuckDBResult> Query(std::string query);

  private:
	DuckDB &database;
};

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
	std::vector<TypeId>& types() { return collection.types; }

	ChunkCollection collection;

	bool success;
	std::string error;

  private:
	DuckDBResult(const DuckDBResult &) = delete;
};
} // namespace duckdb
