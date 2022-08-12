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
	PreservedError();
	explicit PreservedError(std::exception &exception);
	explicit PreservedError(const string &message);
	PreservedError(const Exception &exception);

	bool initialized;
	ExceptionType type;
	string message;

public:
	//! To comply with string usage
	bool empty() const;
	Exception ToException() const;
	operator bool();
};

} // namespace duckdb
