//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_error_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/span.hpp"

namespace duckdb {
class ParsedExpression;

class QueryErrorContext {
public:
	QueryErrorContext(const ParsedExpression &expr); // NOLINT: allow implicit conversion from expression
	explicit QueryErrorContext(Span query_location_p = Span()) : query_location(query_location_p) {
	}

	//! The source span in which the error should be thrown
	Span query_location;

public:
	static string Format(const string &query, const string &error_message, optional_idx error_loc,
	                     idx_t error_length = 0, bool add_line_indicator = true);
};

} // namespace duckdb
