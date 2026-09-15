#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"

namespace duckdb {

class ClientContext;
class DialectExtension;
class GrammarExtension;

using compiled_rules_map_t = case_insensitive_map_t<unique_ptr<CompiledGrammarRule>>;

struct CompiledGrammar {
public:
	CompiledGrammar(MatcherAllocator &&allocator, unique_ptr<PEGKeywordHelper> &&keyword_helper,
	                unique_ptr<Tokenizer> &&tokenizer, compiled_rules_map_t &&rules, const Matcher &program_matcher,
	                const Matcher &top_level_statement_matcher);
	static shared_ptr<CompiledGrammar>
	Create(const case_insensitive_map_t<reference<GrammarExtension>> &grammar_extensions);

public:
	const Matcher &ProgramMatcher() const {
		return program_matcher;
	}
	const Matcher &TopLevelStatementMatcher() const {
		return top_level_statement_matcher;
	}
	const PEGKeywordHelper &GetKeywordHelper() const {
		return *keyword_helper;
	}
	const Tokenizer &GetTokenizer() const {
		return *tokenizer;
	}
	optional_ptr<const CompiledGrammarRule> GetRule(const string &rule_name) const;

public:
	static shared_ptr<CompiledGrammar> Get(ClientContext &context);
	//! Compile the base DuckDB grammar.
	static shared_ptr<CompiledGrammar> Create();
	//! Compile a grammar for the selected extensions without changing the client configuration.
	static shared_ptr<CompiledGrammar> Create(const ClientContext &context,
	                                          const case_insensitive_set_t &active_extensions);

private:
	MatcherAllocator allocator;
	unique_ptr<PEGKeywordHelper> keyword_helper;
	unique_ptr<Tokenizer> tokenizer;
	case_insensitive_map_t<unique_ptr<CompiledGrammarRule>> rules;
	const Matcher &program_matcher;
	const Matcher &top_level_statement_matcher;
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
