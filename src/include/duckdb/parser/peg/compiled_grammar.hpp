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
	CompiledGrammar(const ParsedGrammar &grammar, bool has_grammar_changes, idx_t version);

public:
	const Matcher &ProgramMatcher() const {
		return *program_matcher;
	}
	const Matcher &TopLevelStatementMatcher() const {
		return *top_level_statement_matcher;
	}
	const PEGKeywordHelper &GetKeywordHelper() const {
		return keyword_helper;
	}
	const Tokenizer &GetTokenizer() const {
		return tokenizer;
	}
	optional_ptr<const CompiledGrammarRule> GetRule(const string &rule_name) const;
	bool HasGrammarChanges() const {
		return has_grammar_changes;
	}

public:
	static shared_ptr<CompiledGrammar> Get(ClientContext &context);

public:
	idx_t Version() const;

private:
	MatcherAllocator allocator;
	optional_ptr<const Matcher> program_matcher;
	optional_ptr<const Matcher> top_level_statement_matcher;

	unique_ptr<PEGKeywordHelper> owned_keyword_helper;
	const PEGKeywordHelper &keyword_helper;
	Tokenizer tokenizer;
	case_insensitive_map_t<unique_ptr<CompiledGrammarRule>> rules;

private:
	const bool has_grammar_changes;
	const idx_t version;
};

//! Per-database cache holder for the compiled PEG root matcher and transformer factory.
//! Both are always invalidated together, so they share one mutex and one Invalidate() call.
struct ParserCache {
public:
	ParserCache();

public:
	shared_ptr<CompiledGrammar> GetMatcher(optional_ptr<const ClientContext> context = nullptr);
	void Invalidate();

public:
	idx_t LatestParserVersion() const;

private:
	atomic<idx_t> version;
	std::mutex mutex;
	shared_ptr<CompiledGrammar> matcher;
};

} // namespace duckdb
