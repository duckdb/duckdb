#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/keyword_helper/parsed_grammar_keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_config.hpp"

namespace duckdb {

CompiledGrammar::CompiledGrammar(ParserCache &cache, const ParsedGrammar &grammar)
    : owned_keyword_helper(make_uniq<ParsedGrammarKeywordHelper>(grammar)), keyword_helper(*owned_keyword_helper),
      tokenizer(keyword_helper), version(cache.LatestParserVersion()) {
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

ParserCache::ParserCache() : version(0) {
}

static void ValidateParsedGrammarRoots(const ParsedGrammar &grammar) {
	if (!grammar.GetRule("Program")) {
		throw InvalidInputException("Grammar is missing required root rule 'Program'");
	}
	if (!grammar.GetRule("TopLevelStatement")) {
		throw InvalidInputException("Grammar is missing required root rule 'TopLevelStatement'");
	}
}

static void CheckReference(const ParsedGrammar &grammar, const ParsedGrammarRule &parsed_rule,
                           const PEGExpression &expression) {
	if (expression.type == PEGExpression::Type::REFERENCE || expression.type == PEGExpression::Type::FUNCTION_CALL) {
		if (expression.type != PEGExpression::Type::REFERENCE ||
		    !parsed_rule.recipe.parameters.count(expression.text)) {
			if (!StringUtil::CIEquals(expression.text.GetString(), "EndOfInput") &&
			    !grammar.GetRule(expression.text.GetString())) {
				throw InvalidInputException("Grammar rule '%s' references missing rule '%s'", parsed_rule.name,
				                            expression.text.GetString());
			}
		}
	}
	for (auto &child : expression.children) {
		CheckReference(grammar, parsed_rule, child);
	}
}

shared_ptr<CompiledGrammar> ParserCache::GetMatcher(optional_ptr<ClientContext> context) {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
	}

	auto grammar = ParsedGrammar::CreateDefault();
	ValidateParsedGrammarRoots(grammar);
	for (auto &entry : grammar.rules) {
		auto &parsed_rule = *entry.second;
		CheckReference(grammar, parsed_rule, parsed_rule.recipe.expression);
	}

	auto new_matcher = shared_ptr<CompiledGrammar>(new CompiledGrammar(*this, grammar));
	for (auto &entry : grammar.rules) {
		auto &rule = *entry.second;
		new_matcher->rules.emplace(rule.name, make_uniq<CompiledGrammarRule>(rule.name, std::move(rule.transform)));
	}
	MatcherFactory factory(new_matcher->allocator, grammar, *new_matcher);
	new_matcher->program_matcher = factory.CreateRootMatcher("Program");
	new_matcher->top_level_statement_matcher = factory.GetMatcher("TopLevelStatement");

	std::unique_lock<std::mutex> lock(mutex);
	if (!matcher) {
		matcher = std::move(new_matcher);
	}
	return matcher;
}

optional_ptr<const CompiledGrammarRule> CompiledGrammar::GetRule(const string &rule_name) const {
	auto entry = rules.find(rule_name);
	if (entry == rules.end()) {
		return nullptr;
	}
	return *entry->second;
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
