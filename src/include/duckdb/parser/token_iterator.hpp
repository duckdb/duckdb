//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/token_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/peg/matcher_token.hpp"

namespace duckdb {
struct SimpleToken;
class KeywordTable;
class PEGKeywordHelper;

//! Iterates over an already-tokenized query. A root iterator can own its tokens; child iterators
//! reference the same tokens and carry an independent position for speculative parsing.
class TokenIterator {
public:
	DUCKDB_API explicit TokenIterator(unique_ptr<vector<MatcherToken>> owned_tokens);
	DUCKDB_API explicit TokenIterator(vector<MatcherToken> &tokens);
	TokenIterator(const TokenIterator &other) : tokens(other.tokens), position(other.position) {
	}
	DUCKDB_API TokenIterator(TokenIterator &&other) noexcept;
	TokenIterator &operator=(const TokenIterator &) = delete;
	TokenIterator &operator=(TokenIterator &&) = delete;

	DUCKDB_API bool AtEnd() const;
	DUCKDB_API bool HasMoreStatements() const;
	DUCKDB_API idx_t Size() const;
	DUCKDB_API idx_t EndOffset() const;

	DUCKDB_API const MatcherToken &Previous() const;
	DUCKDB_API const MatcherToken &GetToken(idx_t index) const;

	DUCKDB_API void SetPreviousTokenType(TokenType type);

	idx_t Position() const {
		return position;
	}
	optional_ptr<const MatcherToken> Current() const {
		if (position >= tokens.size()) {
			return nullptr;
		}
		return tokens[position];
	}
	void Advance(idx_t count = 1) {
		if (count > tokens.size() - position) {
			ThrowAdvanceOutOfRange(count);
		}
		position += count;
	}
	void SetPosition(idx_t position_p) {
		if (position_p > tokens.size()) {
			ThrowPositionOutOfRange(position_p);
		}
		position = position_p;
	}
	//! Returns the keyword id of the current token in the given table (INVALID_INDEX if it is not a keyword).
	//! The id is resolved on first use and cached in the token. Requires a current token.
	idx_t CurrentKeywordId(const KeywordTable &table) {
		auto &token = tokens[position];
		if (token.keyword_table.get() != &table) {
			ResolveKeyword(token, table);
		}
		return token.keyword_id;
	}
	//! Returns the keyword categories of the current token according to the given helper, cached in the token
	//! after the first lookup. Requires a current token.
	uint8_t CurrentKeywordCategories(const PEGKeywordHelper &helper) {
		auto &token = tokens[position];
		if (token.keyword_helper.get() != &helper) {
			ResolveKeywordCategories(token, helper);
		}
		return token.keyword_categories;
	}
	//! Moves this iterator to the position of `other`, which must iterate over the same tokens
	void SetPosition(const TokenIterator &other) {
		if (&tokens != &other.tokens) {
			ThrowDifferentTokens();
		}
		position = other.position;
	}

	DUCKDB_API vector<SimpleToken> RemainingTokens() const;
	DUCKDB_API string ToString() const;

private:
	DUCKDB_API static void ResolveKeyword(MatcherToken &token, const KeywordTable &table);
	DUCKDB_API static void ResolveKeywordCategories(MatcherToken &token, const PEGKeywordHelper &helper);
	[[noreturn]] DUCKDB_API void ThrowAdvanceOutOfRange(idx_t count) const;
	[[noreturn]] DUCKDB_API void ThrowDifferentTokens() const;
	[[noreturn]] DUCKDB_API void ThrowPositionOutOfRange(idx_t position_p) const;

private:
	unique_ptr<vector<MatcherToken>> owned_tokens;
	vector<MatcherToken> &tokens;
	idx_t position = 0;
};

} // namespace duckdb
