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
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {
class SQLStatement;

class QueryErrorContext {
public:
	explicit QueryErrorContext(optional_ptr<SQLStatement> statement_ = nullptr,
	                           optional_idx query_location_ = optional_idx())
	    : statement(statement_), query_location(query_location_) {
	}

	//! The query statement
	optional_ptr<SQLStatement> statement;
	//! The location in which the error should be thrown
	optional_idx query_location;

public:
	DUCKDB_API static string Format(const string &query, const string &error_message, optional_idx error_location,
	                                bool add_line_indicator = true);

	DUCKDB_API string FormatErrorRecursive(const string &msg, vector<ExceptionFormatValue> &values);
	template <class T, typename... Args>
	string FormatErrorRecursive(const string &msg, vector<ExceptionFormatValue> &values, T param, Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return FormatErrorRecursive(msg, values, params...);
	}

	template <typename... Args>
	string FormatError(const string &msg, Args... params) {
		vector<ExceptionFormatValue> values;
		return FormatErrorRecursive(msg, values, params...);
	}
};

} // namespace duckdb
