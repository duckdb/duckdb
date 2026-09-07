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

	MatcherResult MatchParseResultInternal(MatchState &state) const override {
		auto token = state.token_iterator.Current();
		if (!token) {
			return MatcherResult::Failure();
		}
		auto &token_text = token->text;
		auto start_offset = optional_idx(token->offset);
		auto token_length = optional_idx(token->length);
		if (!MatchKeyword(state)) {
			return MatcherResult::Failure();
		}
		auto result = state.AllocateParseResult<KeywordParseResult>(token_text, start_offset, token_length);
		if (result.HasParseResult()) {
			result.GetParseResult()->name = name;
		}
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
	idx_t GetKeywordId() const {
		return keyword_id;
	}

private:
	bool MatchKeyword(MatchState &state) const {
		auto token = state.token_iterator.Current();
		if (!token) {
			return false;
		}
		// literals are interned case-insensitively, so comparing ids is a case-insensitive comparison
		if (state.token_iterator.CurrentKeywordId(*keyword_table) == keyword_id) {
			// move to the next token
			state.token_iterator.Advance();
			state.UpdateMaxTokenIndex();
			return true;
		}
		return false;
	}

private:
	friend class MatcherAllocator;
	const string keyword;
	const KeywordInfo info;
	//! Id of the keyword in the grammar's keyword table, assigned by the MatcherAllocator
	idx_t keyword_id = DConstants::INVALID_INDEX;
};

} // namespace duckdb
