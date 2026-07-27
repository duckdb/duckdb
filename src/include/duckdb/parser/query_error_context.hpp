//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_error_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/query_location.hpp"

namespace duckdb {
class ParsedExpression;

class QueryErrorContext {
public:
	QueryErrorContext(const ParsedExpression &expr); // NOLINT: allow implicit conversion from expression
	explicit QueryErrorContext(QueryLocation query_location_p = QueryLocation()) : query_location(query_location_p) {
	}

	//! The source location in which the error should be thrown
	QueryLocation query_location;

public:
	static string Format(const string &query, const string &error_message, optional_idx error_loc,
	                     idx_t error_length = 0, bool add_line_indicator = true);
};

} // namespace duckdb
