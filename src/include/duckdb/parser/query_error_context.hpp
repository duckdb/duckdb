//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_error_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/exception_format_value.hpp"

namespace duckdb {
class SQLStatement;

class QueryErrorContext {
public:
	QueryErrorContext(SQLStatement *statement_ = nullptr, idx_t query_location_ = INVALID_INDEX)
	    : statement(statement_), query_location(query_location_) {
	}

	//! The query statement
	SQLStatement *statement;
	//! The location in which the error should be thrown
	idx_t query_location;

public:
	static string Format(string &query, string error_message, int error_location);

	string FormatErrorRecursive(string msg, vector<ExceptionFormatValue> &values);
	template <class T, typename... Args>
	string FormatErrorRecursive(string msg, vector<ExceptionFormatValue> &values, T param, Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return FormatErrorRecursive(msg, values, params...);
	}

	template <typename... Args> string FormatError(string msg, Args... params) {
		vector<ExceptionFormatValue> values;
		return FormatErrorRecursive(msg, values, params...);
	}
};

} // namespace duckdb
