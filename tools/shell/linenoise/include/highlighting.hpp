//===----------------------------------------------------------------------===//
//                         DuckDB
//
// highlighting.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class tokenType : uint8_t {
	TOKEN_IDENTIFIER,
	TOKEN_NUMERIC_CONSTANT,
	TOKEN_STRING_CONSTANT,
	TOKEN_OPERATOR,
	TOKEN_KEYWORD,
	TOKEN_COMMENT,
	TOKEN_CONTINUATION,
	TOKEN_CONTINUATION_SELECTED
};

struct highlightToken {
	tokenType type;
	size_t start = 0;
	bool search_match = false;
};

class LinenoiseHighlighting {

};

} // namespace duckdb
