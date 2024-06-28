//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/error_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {
class ParsedExpression;
class TableRef;

class ErrorData {
public:
	//! Not initialized, default constructor
	DUCKDB_API ErrorData();
	//! From std::exception
	DUCKDB_API ErrorData(const std::exception &ex); // NOLINT: allow implicit construction from exception
	//! From a raw string and exception type
	DUCKDB_API explicit ErrorData(ExceptionType type, const string &raw_message);
	//! From a raw string
	DUCKDB_API explicit ErrorData(const string &raw_message);

public:
	//! Throw the error
	[[noreturn]] DUCKDB_API void Throw(const string &prepended_message = "") const;
	//! Get the internal exception type of the error
	DUCKDB_API const ExceptionType &Type() const;
	//! Used in clients like C-API, creates the final message and returns a reference to it
	DUCKDB_API const string &Message();
	DUCKDB_API const string &RawMessage() {
		return raw_message;
	}
	DUCKDB_API bool operator==(const ErrorData &other) const;

	inline bool HasError() const {
		return initialized;
	}
	const unordered_map<string, string> &ExtraInfo() const {
		return extra_info;
	}

	DUCKDB_API void AddErrorLocation(const string &query);
	DUCKDB_API void ConvertErrorToJSON();

	DUCKDB_API void AddQueryLocation(optional_idx query_location);
	DUCKDB_API void AddQueryLocation(QueryErrorContext error_context);
	DUCKDB_API void AddQueryLocation(const ParsedExpression &ref);
	DUCKDB_API void AddQueryLocation(const TableRef &ref);

private:
	//! Whether this ErrorData contains an exception or not
	bool initialized;
	//! The ExceptionType of the preserved exception
	ExceptionType type;
	//! The message the exception was constructed with (does not contain the Exception Type)
	string raw_message;
	//! The final message (stored in the preserved error for compatibility reasons with C-API)
	string final_message;
	//! Extra exception info
	unordered_map<string, string> extra_info;

private:
	DUCKDB_API static string SanitizeErrorMessage(string error);
};

} // namespace duckdb
