#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"

namespace duckdb {

MatcherResult Matcher::MatchParseResult(MatchState &state) const {
	state.rule = rule;
	if (state.use_heap_based_parser) {
		MatchStack stack;
		return stack.Execute(*this, state);
	}
	auto result = MatcherResult::Failure();
	if (!state.packrat_cache || !IsPackratMemoized() || !GetPackratId().IsValid()) {
		result = MatchParseResultInternal(state);
	} else {
		auto token_index = state.token_iterator.Position();
		auto cached_result = state.packrat_cache->Lookup(*this, token_index);
		if (cached_result) {
			state.token_iterator.SetPosition(cached_result->token_index_after);
			state.max_token_index = MaxValue(state.max_token_index, cached_result->max_token_index_seen);
			if (cached_result->success) {
				result = MatcherResult::Success(cached_result->result);
			}
		} else {
			auto max_token_index_before = state.GetMaxTokenIndex();
			result = MatchParseResultInternal(state);
			ParserPackratEntry cache_entry;
			cache_entry.success = result.IsSuccess();
			cache_entry.token_index_after = state.token_iterator.Position();
			cache_entry.max_token_index_seen = MaxValue(max_token_index_before, state.GetMaxTokenIndex());
			cache_entry.result = result.GetParseResult();
			state.packrat_cache->Store(*this, token_index, cache_entry);
		}
	}
	return result;
}

SuggestionType Matcher::AddSuggestion(MatchState &state) const {
	auto entry = state.added_suggestions.find(*this);
	if (entry != state.added_suggestions.end()) {
		return SuggestionType::MANDATORY;
	}
	state.added_suggestions.insert(*this);
	return AddSuggestionInternal(state);
}

string Matcher::GetName() const {
	if (name.empty()) {
		return ToString();
	}
	return name;
}

void Matcher::Print() const {
	Printer::Print(ToString());
}

void MatchState::AddSuggestion(MatcherSuggestion suggestion) {
	suggestions.push_back(std::move(suggestion));
}

Matcher &MatcherAllocator::Allocate(unique_ptr<Matcher> matcher) {
	auto &result = *matcher;
	result.packrat_id = optional_idx(matchers.size());
	matchers.push_back(std::move(matcher));
	return result;
}

optional_ptr<ParseResult> ParseResultAllocator::Allocate(unique_ptr<ParseResult> parse_result) {
	auto result_ptr = parse_result.get();
	parse_results.push_back(std::move(parse_result));
	return optional_ptr<ParseResult>(result_ptr);
}

} // namespace duckdb
