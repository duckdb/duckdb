#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "duckdb/parser/peg/inlined_grammar.hpp"
#endif

namespace duckdb {

ParsedGrammar::ParsedGrammar(ParsedGrammar &&other) noexcept : rules(std::move(other.rules)) {
	string_heap.Move(other.string_heap);
}

ParsedGrammar &ParsedGrammar::operator=(ParsedGrammar &&other) noexcept {
	if (this != &other) {
		rules.clear();
		string_heap.Destroy();
		string_heap.Move(other.string_heap);
		rules = std::move(other.rules);
	}
	return *this;
}

ParsedGrammar ParsedGrammar::Parse(const string &grammar) {
	PEGParser parser;
	parser.ParseRules(grammar.c_str());
	ParsedGrammar result;
	for (auto &entry : parser.rules) {
		result.AddParsedRule(ParsedGrammarRule(entry.first, std::move(entry.second)));
	}
	return result;
}

ParsedGrammar ParsedGrammar::CreateDefault() {
#ifdef PEG_PARSER_SOURCE_FILE
	std::ifstream t(PEG_PARSER_SOURCE_FILE);
	std::stringstream buffer;
	buffer << t.rdbuf();
	auto grammar_string = buffer.str();

	const char *grammar = grammar_string.c_str();
#else
	const char *grammar = const_char_ptr_cast(INLINED_PEG_GRAMMAR);
#endif
	auto result = Parse(grammar);
	if (!result.GetRule("EndOfInput")) {
		result.AddParsedRule(ParsedGrammarRule("EndOfInput", PEGRule()));
	}
	PEGTransformerFactory::RegisterDefaultTransforms(result);
	return result;
}

optional_ptr<const ParsedGrammarRule> ParsedGrammar::GetRule(const string &rule_name) const {
	auto entry = rules.find(rule_name);
	if (entry == rules.end()) {
		return nullptr;
	}
	return *entry->second;
}
ParsedGrammarRule &ParsedGrammar::GetMutableRule(const string &rule_name) {
	auto entry = rules.find(rule_name);
	if (entry == rules.end()) {
		throw InvalidInputException("Grammar rule '%s' does not exist", rule_name);
	}
	return *entry->second;
}

ParsedGrammarRule ParsedGrammar::ParseSingleRule(const string &rule_definition) {
	PEGParser parser;
	parser.ParseRules(rule_definition.c_str());
	if (parser.rules.size() != 1) {
		throw InvalidInputException("Expected exactly one PEG rule definition");
	}
	auto &entry = *parser.rules.begin();
	return ParsedGrammarRule(entry.first, std::move(entry.second));
}

void ParsedGrammar::RegisterStrings(PEGRule &rule) {
	string_map_t<idx_t> parameters;
	for (auto &entry : rule.parameters) {
		parameters.emplace(string_heap.AddString(entry.first), entry.second);
	}
	rule.parameters = std::move(parameters);
	for (auto &token : rule.tokens) {
		token.text = string_heap.AddString(token.text);
	}
}

void ParsedGrammar::AddParsedRule(ParsedGrammarRule rule) {
	if (GetRule(rule.name)) {
		throw InvalidInputException("Grammar rule '%s' already exists", rule.name);
	}
	RegisterStrings(rule.recipe);
	auto name = rule.name;
	rules.emplace(std::move(name), make_uniq<ParsedGrammarRule>(std::move(rule)));
}

void ParsedGrammar::AddRule(const string &rule_definition, optional<RuleTransformData> transform_data) {
	auto rule = ParseSingleRule(rule_definition);
	rule.transform_data = std::move(transform_data);
	AddParsedRule(std::move(rule));
}

void ParsedGrammar::ReplaceRule(const string &rule_definition, optional<RuleTransformData> transform_data) {
	auto rule = ParseSingleRule(rule_definition);
	auto entry = rules.find(rule.name);
	if (entry == rules.end()) {
		throw InvalidInputException("Grammar rule '%s' does not exist", rule.name);
	}
	RegisterStrings(rule.recipe);
	rule.transform_data = std::move(transform_data);
	entry->second = make_uniq<ParsedGrammarRule>(std::move(rule));
}

void ParsedGrammar::RemoveRule(const string &rule_name) {
	if (rules.erase(rule_name) == 0) {
		throw InvalidInputException("Grammar rule '%s' does not exist", rule_name);
	}
}

void ParsedGrammar::SetTransform(const string &rule_name, RuleTransformData &&transform_data) {
	auto &rule = GetMutableRule(rule_name);
	rule.transform_data = std::move(transform_data);
}

void ParsedGrammar::SetTrampolineOps(const string &rule_name, const TransformFrameOps &ops) {
	auto &rule = GetMutableRule(rule_name);
	//! FIXME: this should be fixed
	// if (!rule.transform_data) {
	//	throw InvalidInputException("Can't set trampoline ops on a rule (%s) that doesn't have transform data",
	//	                            rule_name);
	//}
	// if (!rule.transform_data->trampoline_transform) {
	//	throw InvalidInputException(
	//	    "Can't set trampoline ops on a rule (%s) that doesn't have a trampoline transform function", rule_name);
	//}
	if (!rule.transform_data) {
		rule.transform_data.emplace();
	}
	rule.transform_data->trampoline_ops = make_shared_ptr<TransformFrameOps>(ops);
}

} // namespace duckdb
