//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_token.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/parser/peg/token_type.hpp"

namespace duckdb {

struct MatcherToken {
	// NOLINTNEXTLINE: allow implicit conversion from text
	MatcherToken(string text_p, idx_t offset_p, TokenType type_p, bool unterminated_p = false)
	    : type(type_p), text(std::move(text_p)), offset(offset_p), unterminated(unterminated_p) {
		length = text.length();
	}

	TokenType type;
	string text;
	idx_t offset = 0;
	idx_t length = 0;
	bool unterminated = false;
	bool preceded_by_newline = false;
	bool preceded_by_block_comment = false;
};

} // namespace duckdb
