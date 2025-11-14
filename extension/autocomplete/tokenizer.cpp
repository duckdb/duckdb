#include "tokenizer.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

BaseTokenizer::BaseTokenizer(const string &sql, vector<MatcherToken> &tokens) : sql(sql), tokens(tokens) {
}

static bool OperatorEquals(const char *str, const char *op, idx_t len, idx_t &op_len) {
	for (idx_t i = 0; i < len; i++) {
		if (str[i] != op[i]) {
			return false;
		}
	}
	op_len = len;
	return true;
}

bool BaseTokenizer::IsSpecialOperator(idx_t pos, idx_t &op_len) const {
	const char *op_start = sql.c_str() + pos;
	if (pos + 2 < sql.size()) {
		if (OperatorEquals(op_start, "->>", 3, op_len)) {
			return true;
		}
	}
	if (pos + 1 >= sql.size()) {
		// 2-byte operators are out-of-bounds
		return false;
	}
	if (OperatorEquals(op_start, "::", 2, op_len)) {
		return true;
	}
	if (OperatorEquals(op_start, ":=", 2, op_len)) {
		return true;
	}
	if (OperatorEquals(op_start, "->", 2, op_len)) {
		return true;
	}
	if (OperatorEquals(op_start, "**", 2, op_len)) {
		return true;
	}
	if (OperatorEquals(op_start, "//", 2, op_len)) {
		return true;
	}
	return false;
}

bool BaseTokenizer::IsSingleByteOperator(char c) {
	switch (c) {
	case '(':
	case ')':
	case '{':
	case '}':
	case '[':
	case ']':
	case ',':
	case '?':
	case '$':
	case '+':
	case '-':
	case '#':
		return true;
	default:
		return false;
	}
}

bool BaseTokenizer::CharacterIsInitialNumber(char c) {
	if (c >= '0' && c <= '9') {
		return true;
	}
	return c == '.';
}

bool BaseTokenizer::CharacterIsNumber(char c) {
	if (CharacterIsInitialNumber(c)) {
		return true;
	}
	switch (c) {
	case 'e': // exponents
	case 'E':
	case '_':
		return true;
	default:
		return false;
	}
}

bool BaseTokenizer::CharacterIsScientific(char c) {
	switch (c) {
	case 'e':
	case 'E':
		return true;
	default:
		return false;
	}
}

bool BaseTokenizer::CharacterIsControlFlow(char c) {
	switch (c) {
	case '\'':
	case '-':
	case ';':
	case '"':
	case '.':
		return true;
	default:
		return false;
	}
}

bool BaseTokenizer::CharacterIsKeyword(char c) {
	if (IsSingleByteOperator(c)) {
		return false;
	}
	if (StringUtil::CharacterIsOperator(c)) {
		return false;
	}
	if (StringUtil::CharacterIsSpace(c)) {
		return false;
	}
	if (CharacterIsControlFlow(c)) {
		return false;
	}
	return true;
}

bool BaseTokenizer::CharacterIsOperator(char c) {
	if (IsSingleByteOperator(c)) {
		return false;
	}
	if (CharacterIsControlFlow(c)) {
		return false;
	}
	return StringUtil::CharacterIsOperator(c);
}

void BaseTokenizer::PushToken(idx_t start, idx_t end) {
	if (start >= end) {
		return;
	}
	string last_token = sql.substr(start, end - start);
	tokens.emplace_back(std::move(last_token), start);
}

bool BaseTokenizer::IsValidDollarTagCharacter(char c) {
	if (c >= 'A' && c <= 'Z') {
		return true;
	}
	if (c >= 'a' && c <= 'z') {
		return true;
	}
	if (c >= '\200' && c <= '\377') {
		return true;
	}
	return false;
}

bool BaseTokenizer::TokenizeInput() {
	auto state = TokenizeState::STANDARD;
	idx_t last_pos = 0;
	string dollar_quote_marker;
	for (idx_t i = 0; i < sql.size(); i++) {
		auto c = sql[i];
		switch (state) {
		case TokenizeState::STANDARD:
			if (c == '\'') {
				state = TokenizeState::STRING_LITERAL;
				last_pos = i;
				break;
			}
			if (c == '"') {
				state = TokenizeState::QUOTED_IDENTIFIER;
				last_pos = i;
				break;
			}
			if (c == ';') {
				// end of statement
				OnStatementEnd(i);
				last_pos = i + 1;
				break;
			}
			if (c == '$') {
				// Dollar-quoted string statement
				if (i + 1 >= sql.size()) {
					// We need more than a single dollar
					break;
				}
				if (sql[i + 1] >= '0' && sql[i + 1] <= '9') {
					// $[numeric] is a parameter, not a dollar-quoted string
					break;
				}
				// Dollar-quoted string
				last_pos = i;
				// Scan until next $
				idx_t next_dollar = 0;
				for (idx_t idx = i + 1; idx < sql.size(); idx++) {
					if (sql[idx] == '$') {
						next_dollar = idx;
						break;
					}
					if (!IsValidDollarTagCharacter(sql[idx])) {
						break;
					}
				}
				if (next_dollar == 0) {
					break;
				}
				state = TokenizeState::DOLLAR_QUOTED_STRING;
				last_pos = i;
				i = next_dollar;
				if (i < sql.size()) {
					// Found a complete marker, store it.
					idx_t marker_start = last_pos + 1;
					dollar_quote_marker = string(sql.begin() + marker_start, sql.begin() + i);
				}
				break;
			}
			if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
				i++;
				state = TokenizeState::SINGLE_LINE_COMMENT;
				break;
			}
			if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
				i++;
				state = TokenizeState::MULTI_LINE_COMMENT;
				break;
			}
			if (StringUtil::CharacterIsSpace(c)) {
				// space character - skip
				last_pos = i + 1;
				break;
			}
			idx_t op_len;
			if (IsSpecialOperator(i, op_len)) {
				// special operator - push the special operator
				tokens.emplace_back(sql.substr(i, op_len), last_pos);
				i += op_len - 1;
				last_pos = i + 1;
				break;
			}
			if (IsSingleByteOperator(c)) {
				// single-byte operator - directly push the token
				tokens.emplace_back(string(1, c), last_pos);
				last_pos = i + 1;
				break;
			}
			if (CharacterIsInitialNumber(c)) {
				// parse a numeric literal
				state = TokenizeState::NUMERIC;
				last_pos = i;
				break;
			}
			if (StringUtil::CharacterIsOperator(c)) {
				state = TokenizeState::OPERATOR;
				last_pos = i;
				break;
			}
			state = TokenizeState::KEYWORD;
			last_pos = i;
			break;
		case TokenizeState::NUMERIC:
			// Check for "always allowed" numeric characters
			if (CharacterIsInitialNumber(c) || c == '_') {
				break; // Continue tokenizing
			}

			// Check for scientific notation marker
			if (CharacterIsScientific(c)) {
				// (e.g., "1ee5" is invalid)
				if (!CharacterIsScientific(sql[i - 1])) {
					break; // Valid 'e' or 'E', continue
				}
			}

			// Check for '+' or '-'
			if (c == '+' || c == '-') {
				if (CharacterIsScientific(sql[i - 1])) {
					break; // Valid, e.g., "1e-5". Continue.
				}
				// Invalid, e.g., "1+5" or "1e5+5". Fall through to stop.
			}

			// --- End of number ---
			// The character 'c' is not a valid part of the number.
			// Stop tokenizing and backtrack as per your original logic.
			while (!CharacterIsInitialNumber(sql[i - 1])) {
				i--;
			}
			PushToken(last_pos, i);
			state = TokenizeState::STANDARD;
			last_pos = i;
			i--;
			break;
		case TokenizeState::OPERATOR:
			// operator literal - check if this is still an operator
			if (!CharacterIsOperator(c)) {
				// not an operator - return to standard state
				PushToken(last_pos, i);
				state = TokenizeState::STANDARD;
				last_pos = i;
				i--;
			}
			break;
		case TokenizeState::KEYWORD:
			// keyword - check if this is still a keyword
			if (!CharacterIsKeyword(c)) {
				// not a keyword - return to standard state
				PushToken(last_pos, i);
				state = TokenizeState::STANDARD;
				last_pos = i;
				i--;
			}
			break;
		case TokenizeState::STRING_LITERAL:
			if (c == '\'') {
				if (i + 1 < sql.size() && sql[i + 1] == '\'') {
					// escaped - skip escape
					i++;
				} else {
					PushToken(last_pos, i + 1);
					last_pos = i + 1;
					state = TokenizeState::STANDARD;
				}
			}
			break;
		case TokenizeState::QUOTED_IDENTIFIER:
			if (c == '"') {
				if (i + 1 < sql.size() && sql[i + 1] == '"') {
					// escaped - skip escape
					i++;
				} else {
					PushToken(last_pos, i + 1);
					last_pos = i + 1;
					state = TokenizeState::STANDARD;
				}
			}
			break;
		case TokenizeState::SINGLE_LINE_COMMENT:
			if (c == '\n' || c == '\r') {
				last_pos = i + 1;
				state = TokenizeState::STANDARD;
			}
			break;
		case TokenizeState::MULTI_LINE_COMMENT:
			if (c == '*' && i + 1 < sql.size() && sql[i + 1] == '/') {
				i++;
				last_pos = i + 1;
				state = TokenizeState::STANDARD;
			}
			break;
		case TokenizeState::DOLLAR_QUOTED_STRING: {
			// Dollar-quoted string -- all that will get us out is a $[marker]$
			if (c != '$') {
				break;
			}
			if (i + 1 >= sql.size()) {
				// No room for the final dollar
				break;
			}
			// Skip to the next dollar symbol
			idx_t start = i + 1;
			idx_t end = start;
			while (end < sql.size() && sql[end] != '$') {
				end++;
			}
			if (end >= sql.size()) {
				// No final dollar, continue as normal
				break;
			}
			if (end - start != dollar_quote_marker.size()) {
				// Length mismatch, cannot match
				break;
			}
			if (sql.compare(start, dollar_quote_marker.size(), dollar_quote_marker) != 0) {
				// marker mismatch
				break;
			}
			// Marker found! Revert to standard state
			size_t full_marker_len = dollar_quote_marker.size() + 2;
			string quoted = sql.substr(last_pos, (start + dollar_quote_marker.size() + 1) - last_pos);
			quoted = "'" + quoted.substr(full_marker_len, quoted.size() - 2 * full_marker_len) + "'";
			tokens.emplace_back(quoted, full_marker_len);
			dollar_quote_marker = string();
			state = TokenizeState::STANDARD;
			i = end;
			last_pos = i + 1;
			break;
		}
		default:
			throw InternalException("unrecognized tokenize state");
		}
	}

	// finished processing - check the final state
	switch (state) {
	case TokenizeState::STRING_LITERAL:
		last_pos++;
		break;
	case TokenizeState::SINGLE_LINE_COMMENT:
	case TokenizeState::MULTI_LINE_COMMENT:
		// no suggestions in comments
		return false;
	default:
		break;
	}
	string last_word = sql.substr(last_pos, sql.size() - last_pos);
	OnLastToken(state, std::move(last_word), last_pos);
	return true;
}

void BaseTokenizer::OnStatementEnd(idx_t pos) {
	tokens.clear();
}

} // namespace duckdb
