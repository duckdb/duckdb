// C++ Interface to DuckDB

#pragma once

#include <string>

namespace duckdb {
	class DuckDB;
	class DuckDBConnection;
	class DuckDBResult;

	class DuckDB {
	  public:
		DuckDB(const char *path);
	};

	class DuckDBConnection {
	  public:
		DuckDBConnection(DuckDB &database);

		DuckDBResult Query(const char *query);
	};

	class DuckDBResult {
	  public:
		DuckDBResult();
		DuckDBResult(std::string error);

		bool GetSuccess() const { return success; }
		const std::string &GetErrorMessage() const { return error; }

	  private:
		bool success;
		std::string error;
	};
}
