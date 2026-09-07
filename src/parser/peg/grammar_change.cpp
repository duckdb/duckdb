#include "duckdb/parser/grammar_change.hpp"

namespace duckdb {

GrammarChange GrammarChange::Create(GrammarChangeType type, string rule_name, string definition,
                                    grammar_transform_function_t transform, grammar_cursor_function_t find_cursor,
                                    terminal_rule_matcher_factory_t matcher_factory) {
	return GrammarChange(type, std::move(rule_name), std::move(definition), std::move(transform),
	                     std::move(find_cursor), std::move(matcher_factory));
}

GrammarChange GrammarChange::AddRule(const string &rule_definition, grammar_transform_function_t transform) {
	auto rule = ParsedGrammar::ParseSingleRule(rule_definition);
	return Create(GrammarChangeType::ADD_RULE, std::move(rule.name), rule_definition, std::move(transform));
}

GrammarChange GrammarChange::AddChoice(const string &rule_name, const string &choice,
                                       grammar_cursor_function_t find_cursor) {
	return Create(GrammarChangeType::ADD_CHOICE, rule_name, choice, nullptr, std::move(find_cursor));
}

GrammarChange GrammarChange::PrependChoice(const string &rule_name, const string &choice,
                                           grammar_cursor_function_t find_cursor) {
	return Create(GrammarChangeType::PREPEND_CHOICE, rule_name, choice, nullptr, std::move(find_cursor));
}

GrammarChange GrammarChange::RemoveChoice(const string &rule_name, grammar_cursor_function_t find_cursor) {
	return Create(GrammarChangeType::REMOVE_CHOICE, rule_name, string(), nullptr, std::move(find_cursor));
}

GrammarChange GrammarChange::ReplaceChoice(const string &rule_name, const string &choice,
                                           grammar_cursor_function_t find_cursor) {
	return Create(GrammarChangeType::REPLACE_CHOICE, rule_name, choice, nullptr, std::move(find_cursor));
}

GrammarChange GrammarChange::ReplaceRule(const string &rule_definition, grammar_transform_function_t transform) {
	auto rule = ParsedGrammar::ParseSingleRule(rule_definition);
	return Create(GrammarChangeType::REPLACE_RULE, std::move(rule.name), rule_definition, std::move(transform));
}

GrammarChange GrammarChange::SetTransform(const string &rule_name, grammar_transform_function_t transform) {
	return Create(GrammarChangeType::SET_TRANSFORM, rule_name, string(), std::move(transform));
}

GrammarChange GrammarChange::AddTerminalRuleOverride(const string &rule_name,
                                                     terminal_rule_matcher_factory_t matcher_factory) {
	return Create(GrammarChangeType::ADD_TERMINAL_RULE_OVERRIDE, rule_name, string(), nullptr, nullptr,
	              std::move(matcher_factory));
}

void GrammarChange::Apply(ParsedGrammar &grammar) const {
	switch (type) {
	case GrammarChangeType::ADD_RULE:
		grammar.AddRule(definition, transform);
		break;
	case GrammarChangeType::ADD_CHOICE:
		grammar.AddChoice(rule_name, definition, find_cursor);
		break;
	case GrammarChangeType::PREPEND_CHOICE:
		grammar.PrependChoice(rule_name, definition, find_cursor);
		break;
	case GrammarChangeType::REMOVE_CHOICE:
		grammar.RemoveChoice(rule_name, find_cursor);
		break;
	case GrammarChangeType::REPLACE_CHOICE:
		grammar.ReplaceChoice(rule_name, definition, find_cursor);
		break;
	case GrammarChangeType::REPLACE_RULE:
		grammar.ReplaceRule(definition, transform);
		break;
	case GrammarChangeType::SET_TRANSFORM:
		grammar.SetTransform(rule_name, transform);
		break;
	case GrammarChangeType::ADD_TERMINAL_RULE_OVERRIDE:
		grammar.AddTerminalRuleOverride(rule_name, matcher_factory);
		break;
	default:
		throw InternalException("Unsupported grammar change type");
	}
}

} // namespace duckdb
