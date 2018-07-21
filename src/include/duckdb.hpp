// C++ Interface to DuckDB

#pragma once

#include <string>

#include "catalog/catalog.hpp"
#include "execution/datachunk.hpp"

namespace duckdb {
class DuckDB;
class DuckDBConnection;
class DuckDBResult;

class DuckDB {
  public:
	DuckDB(const char *path);

	Catalog catalog;
};

class DuckDBConnection {
  public:
	DuckDBConnection(DuckDB &database);

	std::unique_ptr<DuckDBResult> Query(const char *query);

  private:
	DuckDB &database;
};

class DuckDBResult {
  public:
	DuckDBResult();
	DuckDBResult(std::string error);

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return error; }

	void Print();

	DataChunk data;
	
	bool success;
	std::string error;
  private:
  	DuckDBResult(const DuckDBResult&) = delete;

};
}
