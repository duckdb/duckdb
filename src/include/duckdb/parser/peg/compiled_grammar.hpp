#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"

namespace duckdb {

class ClientContext;
class GrammarExtension;

struct CompiledGrammar {
private:
	CompiledGrammar(const ParsedGrammar &grammar, bool has_grammar_changes);
	static shared_ptr<CompiledGrammar>
	Create(const case_insensitive_map_t<reference<GrammarExtension>> &grammar_extensions);

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
	//! Compile the base DuckDB grammar.
	static shared_ptr<CompiledGrammar> Create();
	//! Compile a grammar for the selected extensions without changing the client configuration.
	static shared_ptr<CompiledGrammar> Create(const ClientContext &context,
	                                          const case_insensitive_set_t &active_extensions);

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
};

//! Per-database holder for the compiled base grammar.
struct ParserCache {
public:
	shared_ptr<CompiledGrammar> GetMatcher();

private:
	std::mutex mutex;
	shared_ptr<CompiledGrammar> matcher;
};

} // namespace duckdb
