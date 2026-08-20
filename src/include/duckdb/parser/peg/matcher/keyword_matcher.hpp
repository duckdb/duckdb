#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class KeywordMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::KEYWORD;

public:
	explicit KeywordMatcher(string keyword_p, const KeywordInfo &info)
	    : Matcher(TYPE), keyword(std::move(keyword_p)), info(info) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchKeyword(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return nullptr;
		}
		auto &token_text = token->text;
		auto start_offset = optional_idx(token->offset);
		auto token_length = optional_idx(token->length);
		if (!MatchKeyword(state)) {
			return nullptr;
		}
		auto result = state.allocator.Allocate(make_uniq<KeywordParseResult>(token_text, start_offset, token_length));
		result->name = name;
		return result;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		AutoCompleteCandidate candidate(keyword, SuggestionState::SUGGEST_KEYWORD, info.score_bonus,
		                                CandidateType::KEYWORD);
		candidate.extra_char = info.extra_char;
		state.AddSuggestion(MatcherSuggestion(std::move(candidate)));
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "'" + keyword + "'";
	}

private:
	bool MatchKeyword(MatchState &state) const {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		if (StringUtil::CIEquals(keyword, token->text)) {
			// move to the next token
			state.token_iterator.Advance();
			state.UpdateMaxTokenIndex();
			return true;
		}
		return false;
	}

private:
	const string keyword;
	const KeywordInfo info;
};

} // namespace duckdb
