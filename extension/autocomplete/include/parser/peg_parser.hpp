
#pragma once
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {
enum class PEGRuleType {
	LITERAL,   // literal rule ('Keyword'i)
	REFERENCE, // reference to another rule (Rule)
	OPTIONAL,  // optional rule (Rule?)
	OR,        // or rule (Rule1 / Rule2)
	REPEAT     // repeat rule (Rule1*
};

enum class PEGTokenType {
	LITERAL,       // literal token ('Keyword'i)
	REFERENCE,     // reference token (Rule)
	OPERATOR,      // operator token (/ or )
	FUNCTION_CALL, // start of function call (i.e. Function(...))
	REGEX          // regular expression ([ \t\n\r] or <[a-z_]i[a-z0-9_]i>)
};

struct PEGToken {
	PEGTokenType type;
	string_t text;
};

struct PEGRule {
	string_map_t<idx_t> parameters;
	vector<PEGToken> tokens;

	void Clear() {
		parameters.clear();
		tokens.clear();
	}
};

struct PEGParser {
public:
	void ParseRules(const char *grammar);
	void AddRule(string_t rule_name, PEGRule rule);

	case_insensitive_map_t<PEGRule> rules;
};

enum class PEGParseState {
	RULE_NAME,      // Rule name
	RULE_SEPARATOR, // look for <-
	RULE_DEFINITION // part of rule definition
};

inline bool IsPEGOperator(char c) {
	switch (c) {
	case '/':
	case '?':
	case '(':
	case ')':
	case '*':
	case '+':
	case '!':
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
