#include "parser/peg_parser.hpp"

namespace duckdb {

void PEGParser::AddRule(string_t rule_name, PEGRule rule) {
	auto entry = rules.find(rule_name.GetString());
	if (entry != rules.end()) {
		throw InternalException("Failed to parse grammar - duplicate rule name %s", rule_name.GetString());
	}
	rules.insert(make_pair(rule_name, std::move(rule)));
}

void PEGParser::ParseRules(const char *grammar) {
	string_t rule_name;
	PEGRule rule;
	PEGParseState parse_state = PEGParseState::RULE_NAME;
	idx_t bracket_count = 0;
	bool in_or_clause = false;
	// look for the rules
	idx_t c = 0;
	while (grammar[c]) {
		if (grammar[c] == '#') {
			// comment - ignore until EOL
			while (grammar[c] && !StringUtil::CharacterIsNewline(grammar[c])) {
				c++;
			}
			continue;
		}
		if (parse_state == PEGParseState::RULE_DEFINITION && StringUtil::CharacterIsNewline(grammar[c]) &&
		    bracket_count == 0 && !in_or_clause && !rule.tokens.empty()) {
			// if we see a newline while we are parsing a rule definition we can complete the rule
			AddRule(rule_name, std::move(rule));
			rule_name = string_t();
			rule.Clear();
			// look for the subsequent rule
			parse_state = PEGParseState::RULE_NAME;
			c++;
			continue;
		}
		if (StringUtil::CharacterIsSpace(grammar[c])) {
			// skip whitespace
			c++;
			continue;
		}
		switch (parse_state) {
		case PEGParseState::RULE_NAME: {
			// look for alpha-numerics
			idx_t start_pos = c;
			if (grammar[c] == '%') {
				// rules can start with % (%whitespace)
				c++;
			}
			while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
				c++;
			}
			if (c == start_pos) {
				throw InternalException("Failed to parse grammar - expected an alpha-numeric rule name (pos %d)", c);
			}
			rule_name = string_t(grammar + start_pos, c - start_pos);
			rule.Clear();
			parse_state = PEGParseState::RULE_SEPARATOR;
			break;
		}
		case PEGParseState::RULE_SEPARATOR: {
			if (grammar[c] == '(') {
				if (!rule.parameters.empty()) {
					throw InternalException("Failed to parse grammar - multiple parameters at position %d", c);
				}
				// parameter
				c++;
				idx_t parameter_start = c;
				while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
					c++;
				}
				if (parameter_start == c) {
					throw InternalException("Failed to parse grammar - expected a parameter at position %d", c);
				}
				rule.parameters.insert(
				    make_pair(string_t(grammar + parameter_start, c - parameter_start), rule.parameters.size()));
				if (grammar[c] != ')') {
					throw InternalException("Failed to parse grammar - expected closing bracket at position %d", c);
				}
				c++;
			} else {
				if (grammar[c] != '<' || grammar[c + 1] != '-') {
					throw InternalException("Failed to parse grammar - expected a rule definition (<-) (pos %d)", c);
				}
				c += 2;
				parse_state = PEGParseState::RULE_DEFINITION;
			}
			break;
		}
		case PEGParseState::RULE_DEFINITION: {
			// we parse either:
			// (1) a literal ('Keyword'i)
			// (2) a rule reference (Rule)
			// (3) an operator ( '(' '/' '?' '*' ')' '+')
			in_or_clause = false;
			if (grammar[c] == '\'') {
				// parse literal
				c++;
				idx_t literal_start = c;
				while (grammar[c] && grammar[c] != '\'') {
					if (grammar[c] == '\\') {
						// escape
						c++;
					}
					c++;
				}
				if (!grammar[c]) {
					throw InternalException("Failed to parse grammar - did not find closing ' (pos %d)", c);
				}
				PEGToken token;
				token.text = string_t(grammar + literal_start, c - literal_start);
				token.type = PEGTokenType::LITERAL;
				rule.tokens.push_back(token);
				c++;
				if (grammar[c] == 'i') {
					throw InternalException("Failed to parse grammar - unexpected \"i\" found in grammar near rule %s",
					                        rule_name.GetString());
				}
			} else if (StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
				// alphanumeric character - this is a rule reference
				idx_t rule_start = c;
				while (grammar[c] && StringUtil::CharacterIsAlphaNumeric(grammar[c])) {
					c++;
				}
				PEGToken token;
				token.text = string_t(grammar + rule_start, c - rule_start);
				if (grammar[c] == '(') {
					// this is a function call
					c++;
					bracket_count++;
					token.type = PEGTokenType::FUNCTION_CALL;
				} else {
					token.type = PEGTokenType::REFERENCE;
				}
				rule.tokens.push_back(token);
			} else if (grammar[c] == '[' || grammar[c] == '<') {
				// regular expression- [^"] or <...>
				idx_t rule_start = c;
				char final_char = grammar[c] == '[' ? ']' : '>';
				while (grammar[c] && grammar[c] != final_char) {
					if (grammar[c] == '\\') {
						// handle escapes
						c++;
					}
					if (grammar[c]) {
						c++;
					}
				}
				c++;
				PEGToken token;
				token.text = string_t(grammar + rule_start, c - rule_start);
				token.type = PEGTokenType::REGEX;
				rule.tokens.push_back(token);
			} else if (IsPEGOperator(grammar[c])) {
				if (grammar[c] == '(') {
					bracket_count++;
				} else if (grammar[c] == ')') {
					if (bracket_count == 0) {
						throw InternalException("Failed to parse grammar - unclosed bracket at position %d in rule %s",
						                        c, rule_name.GetString());
					}
					bracket_count--;
				} else if (grammar[c] == '/') {
					in_or_clause = true;
				}
				// operator - operators are always length 1
				PEGToken token;
				token.text = string_t(grammar + c, 1);
				token.type = PEGTokenType::OPERATOR;
				rule.tokens.push_back(token);
				c++;
			} else {
				throw InternalException("Unrecognized rule contents in rule %s (character %s)", rule_name.GetString(),
				                        string(1, grammar[c]));
			}
		}
		default:
			break;
		}
		if (!grammar[c]) {
			break;
		}
	}
	if (parse_state == PEGParseState::RULE_SEPARATOR) {
		throw InternalException("Failed to parse grammar - rule %s does not have a definition", rule_name.GetString());
	}
	if (parse_state == PEGParseState::RULE_DEFINITION) {
		if (rule.tokens.empty()) {
			throw InternalException("Failed to parse grammar - rule %s is empty", rule_name.GetString());
		}
		AddRule(rule_name, std::move(rule));
	}
}

} // namespace duckdb
