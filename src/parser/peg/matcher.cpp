#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

// uncomment to dynamically read the PEG parser from a file instead of compiling it in (useful for testing)
// #define PEG_PARSER_SOURCE_FILE "duckdb/parser/peg/inlined_grammar.gram"

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
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "duckdb/parser/peg/inlined_grammar.hpp"
#endif

namespace duckdb {

optional_ptr<ParseResult> Matcher::MatchParseResult(MatchState &state) const {
	if (state.packrat_cache && IsPackratMemoized()) {
		return state.packrat_cache->Match(*this, state);
	}
	return MatchParseResultInternal(state);
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

shared_ptr<PEGMatcher> PEGMatcher::Get(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	return PEGMatcher::Get(db);
}

shared_ptr<PEGMatcher> PEGMatcher::Get(DatabaseInstance &db) {
	auto &parser_cache = db.GetParserCache();
	return parser_cache.GetMatcher();
}

shared_ptr<PEGMatcher> ParserCache::GetMatcher() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
	}
	auto new_matcher = make_shared_ptr<PEGMatcher>();
	MatcherFactory factory(new_matcher->allocator);
#ifdef PEG_PARSER_SOURCE_FILE
	std::ifstream t(PEG_PARSER_SOURCE_FILE);
	std::stringstream buffer;
	buffer << t.rdbuf();
	auto grammar_string = buffer.str();

	new_matcher->program_matcher = factory.CreateMatcher(grammar_string.c_str(), "Program");
#else
	new_matcher->program_matcher = factory.CreateMatcher(const_char_ptr_cast(INLINED_PEG_GRAMMAR), "Program");
#endif
	// TopLevelStatement is referenced by Program, so it has already been built and cached.
	new_matcher->top_level_statement_matcher = factory.GetMatcher("TopLevelStatement");
	std::unique_lock<std::mutex> lock(mutex);
	if (!matcher) {
		matcher = std::move(new_matcher);
	}
	return matcher;
}

shared_ptr<PEGTransformerFactory> ParserCache::GetTransformerFactory() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (transformer_factory) {
			return transformer_factory;
		}
	}
	auto new_factory = make_shared_ptr<PEGTransformerFactory>();
	std::unique_lock<std::mutex> lock(mutex);
	if (!transformer_factory) {
		transformer_factory = std::move(new_factory);
	}
	return transformer_factory;
}

void ParserCache::Invalidate() {
	std::unique_lock<std::mutex> lock(mutex);
	matcher = nullptr;
	transformer_factory = nullptr;
}

} // namespace duckdb
