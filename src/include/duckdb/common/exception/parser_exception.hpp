//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception/parser_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class ParserException : public Exception {
public:
	DUCKDB_API explicit ParserException(const string &msg);
	DUCKDB_API explicit ParserException(const string &msg, const unordered_map<string, string> &extra_info);

	template <typename... Args>
	explicit ParserException(const string &msg, Args... params) : ParserException(ConstructMessage(msg, params...)) {
	}

	static ParserException SyntaxError(const string &query, const string &error_message, optional_idx error_location);
};

} // namespace duckdb
