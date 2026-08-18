#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/keyword_helper/duckdb_keyword_helper.hpp"
#include "duckdb/main/database.hpp"
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "duckdb/parser/peg/inlined_grammar.hpp"
#endif

namespace duckdb {

CompiledGrammar::CompiledGrammar() : keyword_helper(DuckDBKeywordHelper::Instance()) {
}

shared_ptr<CompiledGrammar> CompiledGrammar::Get(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	return CompiledGrammar::Get(db);
}

shared_ptr<CompiledGrammar> CompiledGrammar::Get(DatabaseInstance &db) {
	auto &parser_cache = db.GetParserCache();
	return parser_cache.GetMatcher();
}

const PEGTransformerFactory &CompiledGrammar::GetTransformerFactory() {
	return transformer_factory;
}

shared_ptr<CompiledGrammar> ParserCache::GetMatcher() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
	}
	auto new_matcher = make_shared_ptr<CompiledGrammar>();
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

void ParserCache::Invalidate() {
	std::unique_lock<std::mutex> lock(mutex);
	matcher = nullptr;
}

} // namespace duckdb
