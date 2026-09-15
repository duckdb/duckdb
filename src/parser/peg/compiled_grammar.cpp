#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/keyword_helper/parsed_grammar_keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/peg/dialect_extension.hpp"
#include "duckdb/parser/grammar_extension.hpp"

namespace duckdb {

CompiledGrammar::CompiledGrammar(MatcherAllocator &&allocator_p, unique_ptr<PEGKeywordHelper> &&keyword_helper_p,
                                 unique_ptr<Tokenizer> &&tokenizer_p, compiled_rules_map_t &&rules_p,
                                 const Matcher &program_matcher, const Matcher &top_level_statement_matcher)
    : allocator(std::move(allocator_p)), keyword_helper(std::move(keyword_helper_p)), tokenizer(std::move(tokenizer_p)),
      rules(std::move(rules_p)), program_matcher(program_matcher),
      top_level_statement_matcher(top_level_statement_matcher) {
}

shared_ptr<CompiledGrammar> CompiledGrammar::Get(ClientContext &context) {
	auto &client_config = ClientConfig::GetConfig(context);
	auto &callback_manager = ExtensionCallbackManager::Get(context);
	if (client_config.current_dialect) {
		auto dialect_extension = callback_manager.GetDialectExtension(*client_config.current_dialect);
		if (!dialect_extension) {
			throw InternalException("Dialect extension set to '%s' but couldn't be located in the registry",
			                        *client_config.current_dialect);
		}
		return dialect_extension->GetCompiledGrammar(context);
	}
	if (client_config.cached_grammar) {
		return client_config.cached_grammar;
	}
	return DatabaseInstance::GetDatabase(context).GetParserCache().GetMatcher();
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
		if (expression.type != PEGExpression::Type::REFERENCE &&
		    expression.type != PEGExpression::Type::FUNCTION_CALL) {
			break;
		}
		if (expression.type == PEGExpression::Type::REFERENCE && parsed_rule.recipe.parameters.count(expression.text)) {
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

shared_ptr<CompiledGrammar>
CompiledGrammar::Create(const case_insensitive_map_t<reference<GrammarExtension>> &grammar_extensions) {
	auto grammar = ParsedGrammar::CreateDefault();
	for (auto &[_, extension] : grammar_extensions) {
		auto changes = extension.get().GetChanges();
		for (auto &change : changes) {
			change.Apply(grammar);
		}
	}
	ValidateParsedGrammarRoots(grammar);
	for (auto &entry : grammar.rules) {
		auto &parsed_rule = *entry.second;
		auto &expression = entry.second->recipe.expression;
		CheckReference(grammar, parsed_rule, expression);
	}

	auto keyword_helper = make_uniq<ParsedGrammarKeywordHelper>(grammar);
	auto tokenizer = make_uniq<Tokenizer>(*keyword_helper);
	compiled_rules_map_t rules;
	for (auto &entry : grammar.rules) {
		auto &rule = *entry.second;
		rules.emplace(rule.name, make_uniq<CompiledGrammarRule>(rule.name, rule.transform_process));
	}

	MatcherAllocator allocator;
	auto terminal_rule_overrides = grammar.BuildTerminalRuleOverrides(*keyword_helper);
	MatcherFactory factory(allocator, grammar, rules, std::move(terminal_rule_overrides));

	auto &program_matcher = factory.CreateRootMatcher("Program");
	auto &top_level_statement_matcher = factory.GetMatcher("TopLevelStatement");

	auto new_matcher = shared_ptr<CompiledGrammar>(new CompiledGrammar(std::move(allocator), std::move(keyword_helper),
	                                                                   std::move(tokenizer), std::move(rules),
	                                                                   program_matcher, top_level_statement_matcher));
	return new_matcher;
}

shared_ptr<CompiledGrammar> CompiledGrammar::Create() {
	return Create({});
}

shared_ptr<CompiledGrammar> CompiledGrammar::Create(const ClientContext &context,
                                                    const case_insensitive_set_t &active_extensions) {
	case_insensitive_map_t<reference<GrammarExtension>> selected_extensions;
	auto &callback_manager = ExtensionCallbackManager::Get(context);
	for (auto &name : active_extensions) {
		auto grammar_extension = callback_manager.FindGrammarExtension(name);
		if (grammar_extension) {
			selected_extensions.emplace(name, *grammar_extension);
		}
	}
	return Create(selected_extensions);
}

shared_ptr<CompiledGrammar> ParserCache::GetMatcher() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
	}
	auto new_matcher = CompiledGrammar::Create();

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

} // namespace duckdb
