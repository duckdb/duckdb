#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"

namespace duckdb {

struct ParserCache;
class ClientContext;

struct CompiledGrammar {
	friend struct ParserCache;

private:
	explicit CompiledGrammar(ParserCache &cache);

public:
	const Matcher &ProgramMatcher() {
		return *program_matcher;
	}
	const Matcher &TopLevelStatementMatcher() {
		return *top_level_statement_matcher;
	}
	const PEGKeywordHelper &GetKeywordHelper() const {
		return keyword_helper;
	}
	const Tokenizer &GetTokenizer() const {
		return tokenizer;
	}
	optional_ptr<const CompiledGrammarRule> GetRule(const string &rule_name) const;

public:
	static shared_ptr<CompiledGrammar> Get(ClientContext &context);
	static shared_ptr<CompiledGrammar> Get(DatabaseInstance &db);

public:
	idx_t Version() const;

private:
	MatcherAllocator allocator;
	optional_ptr<const Matcher> program_matcher;
	optional_ptr<const Matcher> top_level_statement_matcher;

	//! TODO: this should be a unique_ptr when we allow keyword overrides
	const PEGKeywordHelper &keyword_helper;
	Tokenizer tokenizer;
	case_insensitive_map_t<unique_ptr<CompiledGrammarRule>> rules;

private:
	const idx_t version;
};

//! Per-database cache holder for the compiled PEG root matcher and transformer factory.
//! Both are always invalidated together, so they share one mutex and one Invalidate() call.
struct ParserCache {
public:
	ParserCache();

public:
	shared_ptr<CompiledGrammar> GetMatcher(optional_ptr<ClientContext> context);
	void Invalidate();

public:
	idx_t LatestParserVersion() const;

private:
	atomic<idx_t> version;
	std::mutex mutex;
	shared_ptr<CompiledGrammar> matcher;
};

} // namespace duckdb
