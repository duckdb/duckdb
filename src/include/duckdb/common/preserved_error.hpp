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
	//! Not initialized, default constructor
	DUCKDB_API PreservedError();
	//! From std::exception
	DUCKDB_API explicit PreservedError(const std::exception &exception);
	//! From a raw string
	DUCKDB_API explicit PreservedError(const string &raw_message);
	//! From an Exception
	DUCKDB_API PreservedError(const Exception &exception);

public:
	//! Throw the error
	[[noreturn]] DUCKDB_API void Throw(const string &prepended_message = "") const;
	//! Get the internal exception type of the error
	DUCKDB_API const ExceptionType &Type() const;
	//! Allows adding addition information to the message
	DUCKDB_API PreservedError &AddToMessage(const string &prepended_message);
	//! Used in clients like C-API, creates the final message and returns a reference to it
	DUCKDB_API const string &Message();
	//! Let's us do things like 'if (error)'
	operator bool() const;
	bool operator==(const PreservedError &other) const;

private:
	//! Whether this PreservedError contains an exception or not
	bool initialized;
	//! The ExceptionType of the preserved exception
	ExceptionType type;
	//! The message the exception was constructed with (does not contain the Exception Type)
	string raw_message;
	//! The final message (stored in the preserved error for compatibility reasons with C-API)
	string final_message;

private:
	static string SanitizeErrorMessage(string error);
};

} // namespace duckdb
