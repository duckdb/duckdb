#include "duckdb/parser/token_iterator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

TokenIterator::TokenIterator(unique_ptr<vector<MatcherToken>> owned_tokens_p)
    : owned_tokens(std::move(owned_tokens_p)), tokens(*owned_tokens) {
	if (!owned_tokens) {
		throw InternalException("Cannot construct an owning TokenIterator without tokens");
	}
}

TokenIterator::TokenIterator(vector<MatcherToken> &tokens_p) : tokens(tokens_p) {
}

TokenIterator::TokenIterator(TokenIterator &other) : tokens(other.tokens), position(other.position) {
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

idx_t TokenIterator::Position() const {
	return position;
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

optional_ptr<const MatcherToken> TokenIterator::Current() const {
	if (position >= tokens.size()) {
		return nullptr;
	}
	return tokens[position];
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

void TokenIterator::Advance(idx_t count) {
	if (count > tokens.size() - position) {
		throw InternalException("Cannot advance TokenIterator by %llu tokens from position %llu (size %llu)", count,
		                        position, tokens.size());
	}
	position += count;
}

void TokenIterator::SetPosition(idx_t position_p) {
	if (position_p > tokens.size()) {
		throw InternalException("Token position %llu is out of range (size %llu)", position_p, tokens.size());
	}
	position = position_p;
}

void TokenIterator::SetPosition(const TokenIterator &other) {
	if (&tokens != &other.tokens) {
		throw InternalException("Cannot set TokenIterator position from a different token collection");
	}
	SetPosition(other.position);
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
