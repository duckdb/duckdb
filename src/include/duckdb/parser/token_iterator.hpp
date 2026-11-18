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
class TokenizerBehavior;

//! Iterates over an already-tokenized query. A root iterator can own its tokens; child iterators
//! reference the same tokens and carry an independent position for speculative parsing.
class TokenIterator {
public:
	DUCKDB_API explicit TokenIterator(unique_ptr<vector<MatcherToken>> owned_tokens);
	//! Takes ownership of the tokens the behavior produced (leaving its vector empty) and keeps a pointer to
	//! `behavior.sql`, so Source() is always the string their offsets index into. That string has to outlive the
	//! iterator and its children, the behavior itself does not.
	DUCKDB_API explicit TokenIterator(TokenizerBehavior &behavior);
	DUCKDB_API explicit TokenIterator(vector<MatcherToken> &tokens);
	DUCKDB_API TokenIterator(const TokenIterator &other)
	    : tokens(other.tokens), source(other.source), position(other.position) {
	}
	DUCKDB_API TokenIterator(TokenIterator &&other) noexcept;
	TokenIterator &operator=(const TokenIterator &) = delete;
	TokenIterator &operator=(TokenIterator &&) = delete;

	DUCKDB_API bool AtEnd() const {
		auto current = Current();
		return !current || current->type == TokenType::END_OF_INPUT;
	}
	DUCKDB_API bool HasMoreStatements() const;
	DUCKDB_API idx_t Position() const {
		return position;
	}
	DUCKDB_API idx_t Size() const {
		return tokens.size();
	}
	DUCKDB_API idx_t EndOffset() const;
	//! Byte offset just past the last consumed token, or 0 if no token has been consumed yet
	DUCKDB_API idx_t ConsumedEndOffset() const;
	//! The text the tokens were cut from (token offsets index into it), when built from a TokenizerBehavior
	DUCKDB_API optional_ptr<const string> Source() const {
		return source;
	}

	DUCKDB_API optional_ptr<const MatcherToken> Current() const {
		if (position >= tokens.size()) {
			return nullptr;
		}
		return tokens[position];
	}
	LiteralInfo CurrentLiteralInfo(const GrammarLiteralTable &table) {
		if (position >= tokens.size()) {
			return LiteralInfo();
		}
		return tokens[position].GetLiteralInfo(table);
	}
	DUCKDB_API const MatcherToken &Previous() const;
	const MatcherToken &GetToken(idx_t index) const {
		if (index >= tokens.size()) {
			ThrowIndexError(index);
		}
		return tokens[index];
	}

	void Advance(idx_t count = 1) {
		if (count > tokens.size() - position) {
			ThrowAdvanceError(count);
		}
		position += count;
	}
	void SetPosition(idx_t position_p) {
		if (position_p > tokens.size()) {
			ThrowPositionError(position_p);
		}
		position = position_p;
	}
	void SetPosition(const TokenIterator &other) {
		if (&tokens != &other.tokens) {
			ThrowCollectionError();
		}
		SetPosition(other.position);
	}
	void SetPreviousTokenType(TokenType type) {
		if (position == 0) {
			ThrowNoPreviousError();
		}
		tokens[position - 1].type = type;
	}
	DUCKDB_API void ThrowIndexError(idx_t index) const;
	DUCKDB_API void ThrowAdvanceError(idx_t count) const;
	DUCKDB_API void ThrowPositionError(idx_t position_p) const;
	DUCKDB_API void ThrowCollectionError() const;
	DUCKDB_API void ThrowNoPreviousError() const;

	DUCKDB_API vector<SimpleToken> RemainingTokens() const;
	DUCKDB_API string ToString() const;

private:
	unique_ptr<vector<MatcherToken>> owned_tokens;
	vector<MatcherToken> &tokens;
	optional_ptr<const string> source;
	idx_t position = 0;
};

} // namespace duckdb
