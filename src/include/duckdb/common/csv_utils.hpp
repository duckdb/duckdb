//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/csv_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct CSVUtils {
	static void WriteQuoteOrEscape(WriteStream &writer, char quote_or_escape);
	static string AddEscapes(char to_be_escaped, char escape, const string &val);
	static bool RequiresQuotes(const char *str, idx_t len, vector<string> &null_str,
	                           unsafe_unique_array<bool> &requires_quotes);
	static void WriteQuotedString(WriteStream &writer, const char *str, idx_t len, bool force_quote,
	                              vector<string> &null_str, unsafe_unique_array<bool> &requires_quotes, char quote,
	                              char escape);
};

} // namespace duckdb
