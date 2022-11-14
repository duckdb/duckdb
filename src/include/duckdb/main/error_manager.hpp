//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/error_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;

enum class ErrorType : uint16_t {
	// error message types
	UNSIGNED_EXTENSION = 0,
	INVALIDATED_TRANSACTION = 1,
	INVALIDATED_DATABASE = 2,

	// this should always be the last value
	ERROR_COUNT,
	INVALID = 65535,
};

//! The error manager class is responsible for formatting error messages
//! It allows for error messages to be overridden by extensions and clients
class ErrorManager {
public:
	template <typename... Args>
	string FormatException(ErrorType error_type, Args... params) {
		vector<ExceptionFormatValue> values;
		return FormatExceptionRecursive(error_type, values, params...);
	}

	DUCKDB_API string FormatExceptionRecursive(ErrorType error_type, vector<ExceptionFormatValue> &values);

	template <class T, typename... Args>
	string FormatExceptionRecursive(ErrorType error_type, vector<ExceptionFormatValue> &values, T param,
	                                Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return FormatExceptionRecursive(error_type, values, params...);
	}

	template <typename... Args>
	static string FormatException(ClientContext &context, ErrorType error_type, Args... params) {
		return Get(context).FormatException(error_type, params...);
	}

	DUCKDB_API static string InvalidUnicodeError(const string &input, const string &context);

	//! Adds a custom error for a specific error type
	void AddCustomError(ErrorType type, string new_error);

	DUCKDB_API static ErrorManager &Get(ClientContext &context);
	DUCKDB_API static ErrorManager &Get(DatabaseInstance &context);

private:
	map<ErrorType, string> custom_errors;
};

} // namespace duckdb
