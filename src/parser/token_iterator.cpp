#include "duckdb/parser/token_iterator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"

namespace duckdb {

TokenIterator::TokenIterator(unique_ptr<vector<MatcherToken>> owned_tokens_p)
    : owned_tokens(std::move(owned_tokens_p)), tokens(*owned_tokens) {
	if (!owned_tokens) {
		throw InternalException("Cannot construct an owning TokenIterator without tokens");
	}
	// Owned tokens come straight from the tokenizer and keep the literal info it cached on them.
}

TokenIterator::TokenIterator(TokenizerBehavior &behavior)
    : TokenIterator(make_uniq<vector<MatcherToken>>(std::move(behavior.tokens))) {
	source = &behavior.sql;
}

TokenIterator::TokenIterator(vector<MatcherToken> &tokens_p) : tokens(tokens_p) {
	// A new root can receive tokens edited since an earlier match.
	for (auto &token : tokens) {
		token.ResetLiteralInfo();
	}
}

TokenIterator::TokenIterator(TokenIterator &&other) noexcept
    : owned_tokens(std::move(other.owned_tokens)), tokens(other.tokens), source(other.source),
      position(other.position) {
}

bool TokenIterator::HasMoreStatements() const {
	for (idx_t index = position; index < tokens.size(); index++) {
		auto type = tokens[index].type;
		if (type == TokenType::END_OF_INPUT) {
			return false;
		}
		if (type != TokenType::TERMINATOR) {
			return true;
		}
	}
	return false;
}

idx_t TokenIterator::EndOffset() const {
	if (tokens.empty()) {
		return 0;
	}
	auto &last_token = tokens.back();
	return last_token.offset + last_token.length;
}

idx_t TokenIterator::ConsumedEndOffset() const {
	if (position == 0) {
		return 0;
	}
	auto &last_token = Previous();
	return last_token.offset + last_token.length;
}

const MatcherToken &TokenIterator::Previous() const {
	if (position == 0) {
		throw InternalException("TokenIterator has no previous token");
	}
	return GetToken(position - 1);
}

void TokenIterator::ThrowIndexError(idx_t index) const {
	throw InternalException("Token index %llu is out of range (size %llu)", index, tokens.size());
}

void TokenIterator::ThrowAdvanceError(idx_t count) const {
	throw InternalException("Cannot advance TokenIterator by %llu tokens from position %llu (size %llu)", count,
	                        position, tokens.size());
}

void TokenIterator::ThrowPositionError(idx_t position_p) const {
	throw InternalException("Token position %llu is out of range (size %llu)", position_p, tokens.size());
}

void TokenIterator::ThrowCollectionError() const {
	throw InternalException("Cannot set TokenIterator position from a different token collection");
}

void TokenIterator::ThrowNoPreviousError() const {
	throw InternalException("TokenIterator has no previous token to annotate");
}

vector<SimpleToken> TokenIterator::RemainingTokens() const {
	vector<SimpleToken> result;
	result.reserve(tokens.size() - position);
	for (idx_t index = position; index < tokens.size(); index++) {
		result.emplace_back(tokens[index].text, tokens[index].type);
	}
	return result;
}

string TokenIterator::ToString() const {
	string result;
	for (auto &token : tokens) {
		result += token.text + " ";
	}
	return result;
}

} // namespace duckdb
