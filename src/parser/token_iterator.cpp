#include "duckdb/parser/token_iterator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/keyword_table.hpp"

namespace duckdb {

TokenIterator::TokenIterator(unique_ptr<vector<MatcherToken>> owned_tokens_p)
    : owned_tokens(std::move(owned_tokens_p)), tokens(*owned_tokens) {
	if (!owned_tokens) {
		throw InternalException("Cannot construct an owning TokenIterator without tokens");
	}
}

TokenIterator::TokenIterator(vector<MatcherToken> &tokens_p) : tokens(tokens_p) {
}

TokenIterator::TokenIterator(TokenIterator &&other) noexcept
    : owned_tokens(std::move(other.owned_tokens)), tokens(other.tokens), position(other.position) {
}

bool TokenIterator::AtEnd() const {
	auto current = Current();
	return !current || current->type == TokenType::END_OF_INPUT;
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

idx_t TokenIterator::Size() const {
	return tokens.size();
}

idx_t TokenIterator::EndOffset() const {
	if (tokens.empty()) {
		return 0;
	}
	auto &last_token = tokens.back();
	return last_token.offset + last_token.length;
}

const MatcherToken &TokenIterator::Previous() const {
	if (position == 0) {
		throw InternalException("TokenIterator has no previous token");
	}
	return GetToken(position - 1);
}

const MatcherToken &TokenIterator::GetToken(idx_t index) const {
	if (index >= tokens.size()) {
		throw InternalException("Token index %llu is out of range (size %llu)", index, tokens.size());
	}
	return tokens[index];
}

void TokenIterator::ResolveKeyword(MatcherToken &token, const KeywordTable &table) {
	token.keyword_id = table.Lookup(token.text);
	token.keyword_table = table;
}

void TokenIterator::ResolveKeywordCategories(MatcherToken &token, const PEGKeywordHelper &helper) {
	token.keyword_categories = helper.KeywordCategories(token.text);
	token.keyword_helper = helper;
}

void TokenIterator::ThrowDifferentTokens() const {
	throw InternalException("Cannot set TokenIterator position from a different token collection");
}

void TokenIterator::ThrowAdvanceOutOfRange(idx_t count) const {
	throw InternalException("Cannot advance TokenIterator by %llu tokens from position %llu (size %llu)", count,
	                        position, tokens.size());
}

void TokenIterator::ThrowPositionOutOfRange(idx_t position_p) const {
	throw InternalException("Token position %llu is out of range (size %llu)", position_p, tokens.size());
}

void TokenIterator::SetPreviousTokenType(TokenType type) {
	if (position == 0) {
		throw InternalException("TokenIterator has no previous token to annotate");
	}
	tokens[position - 1].type = type;
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
