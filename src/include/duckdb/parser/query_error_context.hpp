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
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

class QueryErrorContext {
public:
	explicit QueryErrorContext(optional_idx query_location_p = optional_idx()) : query_location(query_location_p) {
	}

	//! The location in which the error should be thrown
	optional_idx query_location;

public:
	static string Format(const string &query, const string &error_message, optional_idx error_loc,
	                     bool add_line_indicator = true);
};

} // namespace duckdb
