#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/keyword_helper/duckdb_keyword_helper.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_config.hpp"
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "duckdb/parser/peg/inlined_grammar.hpp"
#endif

namespace duckdb {

CompiledGrammar::CompiledGrammar(ParserCache &cache)
    : keyword_helper(DuckDBKeywordHelper::Instance()), tokenizer(keyword_helper), version(cache.LatestParserVersion()) {
}

idx_t CompiledGrammar::Version() const {
	return version;
}

shared_ptr<CompiledGrammar> CompiledGrammar::Get(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &client_config = ClientConfig::GetConfig(context);
	auto &cache = db.GetParserCache();
	if (!client_config.cached_grammar || client_config.cached_grammar->Version() != cache.LatestParserVersion()) {
		client_config.cached_grammar = cache.GetMatcher(context);
	}
	return client_config.cached_grammar;
}

shared_ptr<CompiledGrammar> CompiledGrammar::Get(DatabaseInstance &db) {
	auto &parser_cache = db.GetParserCache();
	return parser_cache.GetMatcher(nullptr);
}

const PEGTransformerFactory &CompiledGrammar::GetTransformerFactory() {
	return transformer_factory;
}

ParserCache::ParserCache() : version(0) {
}

shared_ptr<CompiledGrammar> ParserCache::GetMatcher(optional_ptr<ClientContext> context) {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
	}
	auto new_matcher = shared_ptr<CompiledGrammar>(new CompiledGrammar(*this));
	MatcherFactory factory(new_matcher->allocator, new_matcher->GetKeywordHelper());
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

idx_t ParserCache::LatestParserVersion() const {
	return version;
}

void ParserCache::Invalidate() {
	std::unique_lock<std::mutex> lock(mutex);
	matcher = nullptr;
	++version;
}

} // namespace duckdb
