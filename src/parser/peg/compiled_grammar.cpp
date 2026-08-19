#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/keyword_helper/parsed_grammar_keyword_helper.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/parser_change.hpp"

namespace duckdb {

CompiledGrammar::CompiledGrammar(const ParsedGrammar &grammar, bool has_grammar_changes_p, idx_t version_p)
    : owned_keyword_helper(make_uniq<ParsedGrammarKeywordHelper>(grammar)), keyword_helper(*owned_keyword_helper),
      tokenizer(keyword_helper), has_grammar_changes(has_grammar_changes_p), version(version_p) {
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
	do {
		if (expression.kind != PEGExpression::Kind::REFERENCE &&
		    expression.kind != PEGExpression::Kind::FUNCTION_CALL) {
			break;
		}
		if (expression.kind == PEGExpression::Kind::REFERENCE && parsed_rule.recipe.parameters.count(expression.text)) {
			break;
		}
		if (StringUtil::CIEquals(expression.text.GetString(), "EndOfInput")) {
			break;
		}
		if (!grammar.GetRule(expression.text.GetString())) {
			throw InvalidInputException("Grammar rule '%s' references missing rule '%s'", parsed_rule.name,
			                            expression.text.GetString());
		}
	} while (false);
	for (auto &child : expression.children) {
		CheckReference(grammar, parsed_rule, child);
	}
}

terminal_rule_overrides_t ParsedGrammar::BuildTerminalRuleOverrides(const PEGKeywordHelper &keyword_helper) const {
	terminal_rule_overrides_t overrides;
	//===--------------------------------------------------------------------===//
	// START GENERATED RULE OVERRIDES
	//===--------------------------------------------------------------------===//
	AddTerminalRuleOverride(overrides, "Identifier",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper));
	AddTerminalRuleOverride(overrides, "ReservedIdentifier",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper));
	AddTerminalRuleOverride(overrides, "CatalogName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_CATALOG_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "SchemaName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "ReservedSchemaName",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "TableName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "ReservedTableName",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_TABLE_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "ColumnName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_COLUMN_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "ReservedColumnName",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_COLUMN_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "IndexName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper));
	AddTerminalRuleOverride(overrides, "ReservedIndexName",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper));
	AddTerminalRuleOverride(overrides, "SequenceName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper));
	AddTerminalRuleOverride(
	    overrides, "FunctionName",
	    make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME, keyword_helper));
	AddTerminalRuleOverride(
	    overrides, "ReservedFunctionName",
	    make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "ReservedKeyword",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper));
	AddTerminalRuleOverride(overrides, "TableFunctionName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_FUNCTION_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "TypeName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TYPE_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "ReservedTypeName",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_TYPE_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "PragmaName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_PRAGMA_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "SettingName",
	                        make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SETTING_NAME, keyword_helper));
	AddTerminalRuleOverride(overrides, "CopyOptionName",
	                        make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper));
	AddTerminalRuleOverride(overrides, "NumberLiteral", make_uniq<NumberLiteralMatcher>());
	AddTerminalRuleOverride(overrides, "StringLiteral", make_uniq<StringLiteralMatcher>());
	AddTerminalRuleOverride(overrides, "OperatorLiteral", make_uniq<OperatorMatcher>());
	//===--------------------------------------------------------------------===//
	// END GENERATED RULE OVERRIDES
	//===--------------------------------------------------------------------===//

	AddTerminalRuleOverride(overrides, "EndOfInput", make_uniq<EndOfInputMatcher>());
	for (auto &callback : terminal_rule_override_callbacks) {
		callback(keyword_helper, overrides);
	}
	return overrides;
}

shared_ptr<CompiledGrammar> ParserCache::GetMatcher(optional_ptr<const ClientContext> context) {
	idx_t parser_version;
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
		parser_version = version;
	}

	vector<shared_ptr<ParserChange>> parser_changes;
	if (context) {
		for (auto &change : ExtensionCallbackManager::Get(*context).ParserChanges()) {
			parser_changes.push_back(change);
		}
	}

	auto grammar = ParsedGrammar::CreateDefault();
	bool has_grammar_changes = false;
	for (auto &change : parser_changes) {
		switch (change->type) {
		case ParserChangeType::GRAMMAR:
			has_grammar_changes = true;
			change->Apply(grammar);
			break;
		default:
			throw InternalException("Unsupported parser change type");
		}
	}
	ValidateParsedGrammarRoots(grammar);
	for (auto &entry : grammar.rules) {
		auto &parsed_rule = *entry.second;
		auto &expression = entry.second->recipe.expression;
		CheckReference(grammar, parsed_rule, expression);
	}

	auto new_matcher = shared_ptr<CompiledGrammar>(new CompiledGrammar(grammar, has_grammar_changes, parser_version));
	for (auto &entry : grammar.rules) {
		auto &rule = *entry.second;
		new_matcher->rules.emplace(rule.name, make_uniq<CompiledGrammarRule>(rule.name, rule.transform));
	}
	auto terminal_rule_overrides = grammar.BuildTerminalRuleOverrides(new_matcher->GetKeywordHelper());
	MatcherFactory factory(new_matcher->allocator, grammar, *new_matcher, std::move(terminal_rule_overrides));
	new_matcher->program_matcher = factory.CreateRootMatcher("Program");
	new_matcher->top_level_statement_matcher = factory.GetMatcher("TopLevelStatement");

	std::unique_lock<std::mutex> lock(mutex);
	if (version == parser_version) {
		if (matcher) {
			return matcher;
		}
		matcher = new_matcher;
	}
	return new_matcher;
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
