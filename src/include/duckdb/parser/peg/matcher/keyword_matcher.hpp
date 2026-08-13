#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class KeywordMatcher : public Matcher {
public:
	static constexpr MatcherType TYPE = MatcherType::KEYWORD;

public:
	explicit KeywordMatcher(string keyword_p, int32_t score_bonus = 0, char extra_char = '\0')
	    : Matcher(TYPE), keyword(std::move(keyword_p)), score_bonus(score_bonus), extra_char(extra_char) {
	}

	MatchResultType Match(MatchState &state) const override {
		if (!MatchKeyword(state)) {
			return MatchResultType::FAIL;
		}
		return MatchResultType::SUCCESS;
	}

	optional_ptr<ParseResult> MatchParseResultInternal(MatchState &state) const override {
		if (state.token_index >= state.tokens.size()) {
			return nullptr;
		}
		auto &token_text = state.tokens[state.token_index].text;
		auto start_offset = optional_idx(state.tokens[state.token_index].offset);
		auto token_length = optional_idx(state.tokens[state.token_index].length);
		if (!MatchKeyword(state)) {
			return nullptr;
		}
		auto result = state.allocator.Allocate(make_uniq<KeywordParseResult>(token_text, start_offset, token_length));
		result->name = name;
		return result;
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		AutoCompleteCandidate candidate(keyword, SuggestionState::SUGGEST_KEYWORD, score_bonus, CandidateType::KEYWORD);
		candidate.extra_char = extra_char;
		state.AddSuggestion(MatcherSuggestion(std::move(candidate)));
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "'" + keyword + "'";
	}

private:
	bool MatchKeyword(MatchState &state) const {
		if (state.token_index >= state.tokens.size()) {
			return false;
		}
		auto &token = state.tokens[state.token_index];
		if (StringUtil::CIEquals(keyword, token.text)) {
			// move to the next token
			state.token_index++;
			state.UpdateMaxTokenIndex();
			return true;
		}
		return false;
	}

private:
	string keyword;
	int32_t score_bonus;
	char extra_char;
};

} // namespace duckdb
