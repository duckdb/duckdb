//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/preserved_error.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class PreservedError {
public:
	DUCKDB_API PreservedError();
	DUCKDB_API explicit PreservedError(const std::exception &exception);
	DUCKDB_API explicit PreservedError(const string &message);
	DUCKDB_API PreservedError(const Exception &exception);

	bool initialized;
	ExceptionType type;
	string message;

public:
	DUCKDB_API Exception ToException() const;
	operator bool();
};

} // namespace duckdb
