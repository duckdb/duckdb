//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/quote_rules.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {
//! Different Rules regarding possible combinations of Quote and Escape Values for CSV Dialects.
//! Each rule has a comment on the possible combinations.
enum class QuoteRule : uint8_t {
	QUOTES_RFC = 0,   //! quote = " escape = (\0 || " || ')
	QUOTES_OTHER = 1, //! quote = ( " || ' ) escape = '\\'
	NO_QUOTES = 2     //! quote = \0 escape = \0
};
} // namespace duckdb
