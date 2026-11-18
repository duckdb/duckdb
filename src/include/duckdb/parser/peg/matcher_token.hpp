//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/matcher_token.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/parser/peg/token_type.hpp"
#include "duckdb/parser/peg/grammar_literal_table.hpp"

namespace duckdb {

//! Token classes for FIRST-set checks, derived from the TokenType assigned by the tokenizer
//! Each class is a superset of the tokens the corresponding atomic matchers accept
struct MatcherTokenClass {
	static constexpr uint8_t WORD = 1;
	static constexpr uint8_t OPERATOR = 2;
	//! an operator without a precedence level of its own in the grammar
	static constexpr uint8_t GENERIC_OPERATOR = 4;
	static constexpr uint8_t STRING = 8;
	static constexpr uint8_t NUMBER = 16;
};

//! A token class that the caller already derived for the text it passes to a MatcherToken
struct PrecomputedTokenClass {
	explicit PrecomputedTokenClass(uint8_t value_p) : value(value_p) {
	}
	uint8_t value;
};

//! The token class of a token as emitted by the tokenizer (implemented in base_tokenizer.cpp)
DUCKDB_API uint8_t ComputeMatcherTokenClass(TokenType type, const string &text);

//! text, length and token_class are set together on construction: replace a token instead of editing its text
struct MatcherToken {
	// NOLINTNEXTLINE: allow implicit conversion from text
	MatcherToken(string text_p, idx_t offset_p, TokenType type_p, bool unterminated_p = false)
	    : type(type_p), text(std::move(text_p)), offset(offset_p), unterminated(unterminated_p) {
		length = text.length();
		token_class = ComputeMatcherTokenClass(type, text);
	}
	//! Builds the text in place from a range of the input
	MatcherToken(const char *data, idx_t size, idx_t offset_p, TokenType type_p, bool unterminated_p = false)
	    : type(type_p), text(data, size), offset(offset_p), length(size), unterminated(unterminated_p) {
		const bool is_word = type == TokenType::KEYWORD || type == TokenType::IDENTIFIER;
		token_class = is_word ? MatcherTokenClass::WORD : ComputeMatcherTokenClass(type, text);
	}
	MatcherToken(const char *data, idx_t size, idx_t offset_p, TokenType type_p, PrecomputedTokenClass token_class_p)
	    : type(type_p), text(data, size), offset(offset_p), length(size), token_class(token_class_p.value) {
	}

	TokenType type;
	string text;
	idx_t offset = 0;
	idx_t length = 0;
	bool unterminated = false;
	bool preceded_by_newline = false;
	bool preceded_by_block_comment = false;
	//! MatcherTokenClass bits - fixed at tokenization, unaffected by later re-typing of the token
	uint8_t token_class = 0;

	bool HasLiteralInfo(const GrammarLiteralTable &table) const {
		return literal_table_id == table.CacheId();
	}
	//! The tokenizer looks every token up once; later lookups against the same table hit this cache
	void SetLiteralInfo(const GrammarLiteralTable &table, LiteralInfo info) {
		literal_info = info;
		literal_table_id = table.CacheId();
	}
	//! Whether the literal info of this table is cached on the token
	LiteralInfo GetLiteralInfo(const GrammarLiteralTable &table) {
		if (literal_table_id != table.CacheId()) {
			RefreshLiteralInfo(table);
		}
		return literal_info;
	}
	//! The table lookup on a cache miss, kept out of line so the lookahead tests that call GetLiteralInfo stay small
	DUCKDB_API void RefreshLiteralInfo(const GrammarLiteralTable &table);

	void ResetLiteralInfo() {
		literal_table_id = 0;
	}

private:
	LiteralInfo literal_info;
	uint64_t literal_table_id = 0;
};

} // namespace duckdb
