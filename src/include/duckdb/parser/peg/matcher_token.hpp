//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_token.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/peg/token_type.hpp"

namespace duckdb {
class KeywordTable;
class PEGKeywordHelper;

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
	//! Id of the token in the grammar's KeywordTable (INVALID_INDEX if it is not a literal), resolved on first use
	//! and cached here; `keyword_table` records which table the id belongs to
	optional_ptr<const KeywordTable> keyword_table;
	idx_t keyword_id = DConstants::INVALID_INDEX;
	//! Keyword categories of the token (see PEGKeywordHelper::KeywordCategories), cached in the same way
	optional_ptr<const PEGKeywordHelper> keyword_helper;
	uint8_t keyword_categories = 0;
};

} // namespace duckdb
