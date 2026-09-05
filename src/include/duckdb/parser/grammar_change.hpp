//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/grammar_change.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/parsed_grammar.hpp"

namespace duckdb {

struct CompiledGrammar;

enum class GrammarChangeType : uint8_t {
	ADD_RULE,
	ADD_CHOICE,
	PREPEND_CHOICE,
	REMOVE_CHOICE,
	REPLACE_CHOICE,
	REPLACE_RULE,
	SET_TRANSFORM,
	ADD_TERMINAL_RULE_OVERRIDE
};

//! A structured change to apply while constructing a PEG grammar.
class GrammarChange {
public:
	DUCKDB_API static GrammarChange AddRule(const string &rule_definition,
	                                        grammar_transform_function_t transform = nullptr);
	DUCKDB_API static GrammarChange AddChoice(const string &rule_name, const string &choice,
	                                          grammar_cursor_function_t find_cursor = nullptr);
	DUCKDB_API static GrammarChange PrependChoice(const string &rule_name, const string &choice,
	                                              grammar_cursor_function_t find_cursor = nullptr);
	DUCKDB_API static GrammarChange RemoveChoice(const string &rule_name, grammar_cursor_function_t find_cursor);
	DUCKDB_API static GrammarChange ReplaceChoice(const string &rule_name, const string &choice,
	                                              grammar_cursor_function_t find_cursor);
	DUCKDB_API static GrammarChange ReplaceRule(const string &rule_definition,
	                                            grammar_transform_function_t transform = nullptr);
	DUCKDB_API static GrammarChange SetTransform(const string &rule_name, grammar_transform_function_t transform);
	DUCKDB_API static GrammarChange AddTerminalRuleOverride(const string &rule_name,
	                                                        terminal_rule_matcher_factory_t matcher_factory);

	GrammarChangeType Type() const {
		return type;
	}
	const string &RuleName() const {
		return rule_name;
	}
	const string &Definition() const {
		return definition;
	}

public:
	void Apply(ParsedGrammar &grammar) const;

private:
	static GrammarChange Create(GrammarChangeType type, string rule_name, string definition,
	                            grammar_transform_function_t transform = nullptr,
	                            grammar_cursor_function_t find_cursor = nullptr,
	                            terminal_rule_matcher_factory_t matcher_factory = nullptr);

	GrammarChange(GrammarChangeType type_p, string rule_name_p, string definition_p,
	              grammar_transform_function_t transform_p, grammar_cursor_function_t find_cursor_p,
	              terminal_rule_matcher_factory_t matcher_factory_p)
	    : type(type_p), rule_name(std::move(rule_name_p)), definition(std::move(definition_p)),
	      transform(std::move(transform_p)), find_cursor(std::move(find_cursor_p)),
	      matcher_factory(std::move(matcher_factory_p)) {
	}

private:
	GrammarChangeType type;
	string rule_name;
	//! A rule definition for rule changes, or a choice definition for choice changes.
	string definition;
	grammar_transform_function_t transform;
	grammar_cursor_function_t find_cursor;
	terminal_rule_matcher_factory_t matcher_factory;
};

} // namespace duckdb
