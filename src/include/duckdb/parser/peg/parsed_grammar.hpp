//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/parsed_grammar.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/transformer/transform_result.hpp"

#include <functional>

namespace duckdb {

class ParseResult;
class PEGTransformer;
class PEGKeywordHelper;
class Matcher;
class GrammarChange;

using grammar_transform_function_t = std::function<unique_ptr<TransformResultValue>(PEGTransformer &, ParseResult &)>;
using grammar_cursor_function_t = std::function<bool(const PEGExpression &)>;
using terminal_rule_overrides_t = case_insensitive_map_t<unique_ptr<Matcher>>;
using terminal_rule_matcher_factory_t = std::function<unique_ptr<Matcher>(const PEGKeywordHelper &)>;
using terminal_rule_override_callback_t = std::function<void(const PEGKeywordHelper &, terminal_rule_overrides_t &)>;

struct ParsedGrammarRule {
	ParsedGrammarRule(string name_p, PEGRule recipe_p) : name(std::move(name_p)), recipe(std::move(recipe_p)) {
	}

	string name;
	PEGRule recipe;
	grammar_transform_function_t transform;
};

//! Mutable, owning representation of a PEG grammar before matcher compilation.
class ParsedGrammar {
public:
	ParsedGrammar() = default;
	DUCKDB_API ParsedGrammar(ParsedGrammar &&other) noexcept;
	DUCKDB_API ParsedGrammar &operator=(ParsedGrammar &&other) noexcept;
	ParsedGrammar(const ParsedGrammar &) = delete;
	ParsedGrammar &operator=(const ParsedGrammar &) = delete;

	DUCKDB_API static ParsedGrammar Parse(const string &grammar);
	DUCKDB_API static ParsedGrammar CreateDefault();

	DUCKDB_API optional_ptr<const ParsedGrammarRule> GetRule(const string &rule_name) const;
	DUCKDB_API void AddRule(const string &rule_definition, grammar_transform_function_t transform = nullptr);
	DUCKDB_API void AddChoice(const string &rule_name, const string &choice,
	                          const grammar_cursor_function_t &find_cursor = nullptr);
	DUCKDB_API void PrependChoice(const string &rule_name, const string &choice,
	                              const grammar_cursor_function_t &find_cursor = nullptr);
	DUCKDB_API void RemoveChoice(const string &rule_name, const grammar_cursor_function_t &find_cursor);
	DUCKDB_API void ReplaceChoice(const string &rule_name, const string &choice,
	                              const grammar_cursor_function_t &find_cursor);
	DUCKDB_API void ReplaceRule(const string &rule_definition, grammar_transform_function_t transform = nullptr);
	DUCKDB_API void SetTransform(const string &rule_name, grammar_transform_function_t transform);
	DUCKDB_API void AddTerminalRuleOverride(const string &rule_name, terminal_rule_matcher_factory_t matcher_factory);

private:
	friend class MatcherFactory;
	friend struct CompiledGrammar;
	friend class PEGTransformerFactory;
	friend class GrammarChange;

	void AddParsedRule(ParsedGrammarRule rule);
	void InsertChoice(const string &rule_name, const string &choice, const grammar_cursor_function_t &find_cursor,
	                  bool prepend);
	void RegisterStrings(PEGRule &rule);
	ParsedGrammarRule &GetMutableRule(const string &rule_name);
	static ParsedGrammarRule ParseSingleRule(const string &rule_definition);
	static void AddTerminalRuleOverride(terminal_rule_overrides_t &overrides, const string &rule_name,
	                                    unique_ptr<Matcher> matcher);
	terminal_rule_overrides_t BuildTerminalRuleOverrides(const PEGKeywordHelper &keyword_helper) const;

	StringHeap string_heap;
	case_insensitive_map_t<unique_ptr<ParsedGrammarRule>> rules;
	vector<terminal_rule_override_callback_t> terminal_rule_override_callbacks;
};

//! Immutable semantic data referenced directly by matchers and parse results.
struct CompiledGrammarRule {
	CompiledGrammarRule(string name_p, grammar_transform_function_t transform_p)
	    : name(std::move(name_p)), transform(std::move(transform_p)) {
	}

	string name;
	grammar_transform_function_t transform;
};

} // namespace duckdb
