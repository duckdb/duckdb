#include "duckdb/parser/peg/keyword_helper/parsed_grammar_keyword_helper.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"

namespace duckdb {

static void PopulateKeywordMap(const ParsedGrammar &grammar, const string &root_rule_name, const string &rule_name,
                               case_insensitive_set_t &keyword_map, case_insensitive_set_t &active_rules) {
	if (!active_rules.insert(rule_name).second) {
		throw InvalidInputException("Keyword grammar rule '%s' contains a recursive reference to rule '%s'",
		                            root_rule_name, rule_name);
	}
	auto rule_p = grammar.GetRule(rule_name);
	if (!rule_p) {
		throw InvalidInputException("No registered data exists for keyword rule '%s'", rule_name);
	}
	auto &rule = *rule_p;
	if (!rule.recipe.parameters.empty()) {
		throw InvalidInputException("Keyword grammar rule '%s' references parameterized rule '%s'", root_rule_name,
		                            rule_name);
	}

	bool expect_keyword = true;
	for (auto &token : rule.recipe.tokens) {
		if (expect_keyword) {
			switch (token.type) {
			case PEGTokenType::LITERAL:
				keyword_map.insert(StringUtil::Lower(token.text.GetString()));
				break;
			case PEGTokenType::REFERENCE:
				PopulateKeywordMap(grammar, root_rule_name, token.text.GetString(), keyword_map, active_rules);
				break;
			default:
				throw InvalidInputException("Keyword grammar rule '%s' contains unsupported token '%s' in rule '%s'",
				                            root_rule_name, token.text.GetString(), rule_name);
			}
		} else if (token.type != PEGTokenType::OPERATOR || token.text.GetString() != "/") {
			throw InvalidInputException("Keyword grammar rule '%s' must contain only alternatives", root_rule_name);
		}
		expect_keyword = !expect_keyword;
	}
	if (expect_keyword) {
		throw InvalidInputException("Keyword grammar rule '%s' ends with an incomplete alternative", root_rule_name);
	}
	active_rules.erase(rule_name);
}

ParsedGrammarKeywordHelper::ParsedGrammarKeywordHelper(const ParsedGrammar &grammar) {
	unordered_map<string, reference<case_insensitive_set_t>> mapping {
	    {"ReservedKeyword", keyword_maps.reserved_keyword_map},
	    {"UnreservedKeyword", keyword_maps.unreserved_keyword_map},
	    {"ColumnNameKeyword", keyword_maps.colname_keyword_map},
	    {"TypeFuncKeyword", keyword_maps.typefunc_keyword_map},
	    {"TypeNameKeyword", keyword_maps.typename_keyword_map},
	};
	for (auto &entry : mapping) {
		case_insensitive_set_t active_rules;
		PopulateKeywordMap(grammar, entry.first, entry.first, entry.second.get(), active_rules);
	}
}

bool ParsedGrammarKeywordHelper::KeywordCategoryType(const string &text, PEGKeywordCategory category) const {
	return keyword_maps.IsKeywordOfCategory(text, category);
}

bool ParsedGrammarKeywordHelper::IsKeyword(const string &text) const {
	return keyword_maps.IsKeyword(text);
}

vector<ParserKeyword> ParsedGrammarKeywordHelper::KeywordList() const {
	return keyword_maps.ToList();
}

} // namespace duckdb
