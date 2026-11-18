#pragma once

#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/matcher.hpp"

namespace duckdb {

//! Matches a dotted name spelled without quotes: 'carlopi.github.io', 'my-website.com'. The tokenizer cuts such a name
//! into words, '.' and '-' tokens; this matcher glues them back together as long as they touch (no whitespace or
//! comment in between) and the result is a dotted name (IsDottedName): labels of words joined by single dashes,
//! separated by single dots. Note that '--' starts a comment for the tokenizer, so a label with two dashes in a row
//! cannot be spelled unquoted at all; it is not a dotted name either way.
class DottedNameMatcher : public AtomicMatcher {
public:
	static constexpr MatcherType TYPE = MatcherType::VARIABLE;

public:
	DottedNameMatcher() : AtomicMatcher(TYPE) {
		name = "DottedName";
	}

	MatcherResult MatchAtomic(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return MatcherResult::Failure();
		}
		auto start_offset = token->offset;
		string text;
		idx_t end_offset = start_offset;
		idx_t token_count = 0;
		while (true) {
			auto next = state.token_iterator.Current();
			if (!next || next->offset != end_offset || !IsNamePart(*next)) {
				break;
			}
			text += next->text;
			end_offset = next->offset + next->length;
			state.token_iterator.Advance();
			token_count++;
		}
		if (token_count == 0 || !IsDottedName(text)) {
			return MatcherResult::Failure();
		}
		state.UpdateMaxTokenIndex();
		state.token_iterator.SetPreviousTokenType(TokenType::IDENTIFIER);
		if (!state.BuildParseResult()) {
			return MatcherResult::Success();
		}
		state.FoldIdentifier(text);
		return state.AllocateParseResult<IdentifierParseResult>(text, optional_idx(start_offset),
		                                                        optional_idx(end_offset - start_offset));
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "DOTTED_NAME";
	}

	void InitializeFirstSet(MatcherFirstSet &first_set, const GrammarLiteralTable &table) const override {
		first_set.class_mask = MatcherTokenClass::WORD | MatcherTokenClass::NUMBER;
	}

	//! A dotted name: two or more labels separated by single dots, each label one or more words joined by single
	//! dashes, each word one or more letters, digits or '_' (bytes outside ASCII count as letters, so that a name like
	//! münchen.de is a name here and gets the manager's answer rather than a syntax error). So no label starts or ends
	//! with '-', and no two dashes touch
	static bool IsDottedName(const string &text) {
		idx_t label_count = 0;
		idx_t word_length = 0;
		for (idx_t i = 0; i <= text.size(); i++) {
			auto c = i < text.size() ? text[i] : '.';
			if (c == '.' || c == '-') {
				// a separator ends a word, which must not be empty
				if (word_length == 0) {
					return false;
				}
				word_length = 0;
				if (c == '.') {
					label_count++;
				}
				continue;
			}
			if (!StringUtil::CharacterIsAlpha(c) && !StringUtil::CharacterIsDigit(c) && c != '_' &&
			    static_cast<unsigned char>(c) < 0x80) {
				return false;
			}
			word_length++;
		}
		return label_count >= 2;
	}

private:
	//! A token that can be part of an unquoted dotted name: a plain word or number, or a lone '.' or '-'
	static bool IsNamePart(const MatcherToken &token) {
		if (token.text.empty()) {
			return false;
		}
		switch (token.type) {
		case TokenType::KEYWORD:
		case TokenType::IDENTIFIER:
		case TokenType::NUMBER_LITERAL:
			return true;
		case TokenType::OPERATOR:
			return token.text == "." || token.text == "-";
		default:
			return false;
		}
	}
};

} // namespace duckdb
