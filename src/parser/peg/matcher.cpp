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

static MatcherResult ExecuteRecursive(MatchInput input) {
	auto &matcher = input.matcher;
	auto &state = input.state;
	state.rule = matcher.GetRule();
	PackratMatchState packrat_state;
	if (PackratMatchState::IsEnabled(matcher, state)) {
		auto cached_result = packrat_state.TryLoadCachedResult(matcher, state);
		if (cached_result) {
			return *cached_result;
		}
	}

	auto process = matcher.StartMatch(state);
	optional<MatcherResult> child_result;
	while (true) {
		auto step = process->Resume(std::move(child_result));
		child_result.reset();
		auto child = step.GetChild();
		if (!child) {
			auto result = step.GetResult();
			packrat_state.StoreResult(matcher, state, result);
			return result;
		}
		child_result = ExecuteRecursive(*child);
	}
}

MatcherResult Matcher::MatchParseResult(MatchState &state) const {
	MatchInput input {*this, state};
	if (state.use_heap_based_parser) {
		MatchStack stack;
		return stack.Execute(input);
	}
	return ExecuteRecursive(input);
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
