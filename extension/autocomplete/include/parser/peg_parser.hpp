#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// --- PEG Parser ---

enum class PEGTokenType {
	LITERAL,       // literal token ('Keyword'i)
	REFERENCE,     // reference token (Rule)
	OPERATOR,      // operator token (/ or )
	FUNCTION_CALL, // start of function call (i.e. Function(...))
	REGEX          // regular expression ([ \t\n\r] or <[a-z_]i[a-z0-9_]i>)
};

struct PEGToken {
	PEGTokenType type;
	string text;
};

struct PEGRule {
	string_map_t<idx_t> parameters;
	vector<PEGToken> tokens;

	void Clear() {
		parameters.clear();
		tokens.clear();
	}
};

class PEGParser {
public:
	PEGParser() = default;

	void AddRule(const string &rule_name, PEGRule rule) {
		auto entry = rules.find(rule_name);
		if (entry != rules.end()) {
			throw InternalException("Failed to parse grammar - duplicate rule name %s", rule_name);
		}
		rules.insert(make_pair(rule_name, std::move(rule)));
	}

	void ParseRules(const char *grammar);
	case_insensitive_map_t<PEGRule> rules;
};

} // namespace duckdb
