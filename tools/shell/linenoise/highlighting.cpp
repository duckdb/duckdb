#include "linenoise.hpp"
#include "linenoise.h"
#include "highlighting.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string.hpp"
#include "shell_highlight.hpp"

#if defined(_WIN32) || defined(__WIN32__) || defined(WIN32)
// disable highlighting on windows (for now?)
#define DISABLE_HIGHLIGHT
#endif

namespace duckdb {

bool Highlighting::IsEnabled() {
	return duckdb_shell::ShellHighlight::IsEnabled();
}

static tokenType convertToken(duckdb::SimplifiedTokenType token_type) {
	switch (token_type) {
	case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
		return tokenType::TOKEN_IDENTIFIER;
	case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
		return tokenType::TOKEN_NUMERIC_CONSTANT;
	case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
		return tokenType::TOKEN_STRING_CONSTANT;
	case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR:
		return tokenType::TOKEN_OPERATOR;
	case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
		return tokenType::TOKEN_KEYWORD;
	case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
		return tokenType::TOKEN_COMMENT;
	default:
		throw duckdb::InternalException("Unrecognized token type");
	}
}

static vector<highlightToken> GetParseTokens(char *buf, size_t len) {
	string sql(buf, len);
	auto parseTokens = duckdb::Parser::Tokenize(sql);

	vector<highlightToken> tokens;
	for (auto &token : parseTokens) {
		highlightToken new_token;
		new_token.type = convertToken(token.type);
		new_token.start = token.start;
		tokens.push_back(new_token);
	}

	if (!tokens.empty() && tokens[0].start > 0) {
		highlightToken new_token;
		new_token.type = tokenType::TOKEN_IDENTIFIER;
		new_token.start = 0;
		tokens.insert(tokens.begin(), new_token);
	}
	if (tokens.empty() && sql.size() > 0) {
		highlightToken new_token;
		new_token.type = tokenType::TOKEN_IDENTIFIER;
		new_token.start = 0;
		tokens.push_back(new_token);
	}
	return tokens;
}

static vector<highlightToken> GetDotCommandTokens(char *buf, size_t len) {
	vector<highlightToken> tokens;

	// identifier token for the dot command itself
	highlightToken dot_token;
	dot_token.type = tokenType::TOKEN_KEYWORD;
	dot_token.start = 0;
	tokens.push_back(dot_token);

	for (idx_t i = 0; i + 1 < len; i++) {
		if (Linenoise::IsSpace(buf[i])) {
			highlightToken argument_token;
			argument_token.type = tokenType::TOKEN_STRING_CONSTANT;
			argument_token.start = i + 1;
			tokens.push_back(argument_token);
		}
	}
	return tokens;
}

vector<highlightToken> Highlighting::Tokenize(char *buf, size_t len, bool is_dot_command, searchMatch *match) {
	vector<highlightToken> tokens;
	if (!is_dot_command) {
		// SQL query - use parser to obtain tokens
		tokens = GetParseTokens(buf, len);
	} else {
		// . command
		tokens = GetDotCommandTokens(buf, len);
	}
	if (match) {
		// we have a search match - insert it into the token list
		// we want to insert a search token with start = match_start, end = match_end
		// first figure out which token type we would have at match_end (if any)
		for (size_t i = 0; i + 1 < tokens.size(); i++) {
			if (tokens[i].start <= match->match_start && tokens[i + 1].start >= match->match_start) {
				// this token begins after the search position, insert the token here
				size_t token_position = i + 1;
				auto end_type = tokens[i].type;
				if (tokens[i].start == match->match_start) {
					// exact start: only set the search match
					tokens[i].search_match = true;
				} else {
					// non-exact start: add a new token
					highlightToken search_token;
					search_token.type = tokens[i].type;
					search_token.start = match->match_start;
					search_token.search_match = true;
					tokens.insert(tokens.begin() + token_position, search_token);
					token_position++;
				}

				// move forwards
				while (token_position < tokens.size() && tokens[token_position].start < match->match_end) {
					// this token is
					// mark this token as a search token
					end_type = tokens[token_position].type;
					tokens[token_position].search_match = true;
					token_position++;
				}
				if (token_position >= tokens.size() || tokens[token_position].start > match->match_end) {
					// insert the token that marks the end of the search
					highlightToken end_token;
					end_token.type = end_type;
					end_token.start = match->match_end;
					tokens.insert(tokens.begin() + token_position, end_token);
					token_position++;
				}
				break;
			}
		}
	}
	return tokens;
}

string Highlighting::HighlightText(char *buf, size_t len, size_t start_pos, size_t end_pos,
                                   const vector<highlightToken> &tokens) {
	duckdb::stringstream ss;
	size_t prev_pos = 0;
	for (size_t i = 0; i < tokens.size(); i++) {
		size_t next = i + 1 < tokens.size() ? tokens[i + 1].start : len;
		if (next < start_pos) {
			// this token is not rendered at all
			continue;
		}

		auto &token = tokens[i];
		size_t start = token.start > start_pos ? token.start : start_pos;
		size_t end = next > end_pos ? end_pos : next;
		if (end <= start) {
			continue;
		}
		if (prev_pos > start) {
#ifdef DEBUG
			throw InternalException("ERROR - Rendering at position %llu after rendering at position %llu\n", start,
			                        prev_pos);
#endif
			Linenoise::Log("ERROR - Rendering at position %llu after rendering at position %llu\n", start, prev_pos);
			continue;
		}
		prev_pos = start;
		std::string text = std::string(buf + start, end - start);

		duckdb_shell::HighlightElementType element_type;
		switch (token.type) {
		case tokenType::TOKEN_KEYWORD:
			element_type = duckdb_shell::HighlightElementType::KEYWORD;
			break;
		case tokenType::TOKEN_NUMERIC_CONSTANT:
			element_type = duckdb_shell::HighlightElementType::NUMERIC_CONSTANT;
			break;
		case tokenType::TOKEN_STRING_CONSTANT:
			element_type = duckdb_shell::HighlightElementType::STRING_CONSTANT;
			break;
		case tokenType::TOKEN_CONTINUATION:
			element_type = duckdb_shell::HighlightElementType::CONTINUATION;
			break;
		case tokenType::TOKEN_CONTINUATION_SELECTED:
			element_type = duckdb_shell::HighlightElementType::CONTINUATION_SELECTED;
			break;
		case tokenType::TOKEN_BRACKET:
			element_type = duckdb_shell::HighlightElementType::BRACKET;
			break;
		case tokenType::TOKEN_ERROR:
			element_type = duckdb_shell::HighlightElementType::ERROR_TOKEN;
			break;
		case tokenType::TOKEN_COMMENT:
			element_type = duckdb_shell::HighlightElementType::COMMENT;
			break;
		default:
			element_type = duckdb_shell::HighlightElementType::NONE;
			break;
		}
		auto &element = duckdb_shell::ShellHighlight::GetHighlightElement(element_type);
		auto color = element.color;
		auto intensity = element.intensity;
		if (token.search_match) {
			// add underline for search matches
			if (intensity == duckdb_shell::PrintIntensity::BOLD) {
				intensity = duckdb_shell::PrintIntensity::BOLD_UNDERLINE;
			} else {
				intensity = duckdb_shell::PrintIntensity::UNDERLINE;
			}
		}
		string terminal_code = duckdb_shell::ShellHighlight::TerminalCode(color, intensity);
		if (terminal_code.empty()) {
			ss << text;
		} else {
			ss << terminal_code;
			ss << text;
			ss << duckdb_shell::ShellHighlight::ResetTerminalCode();
		}
	}
	return ss.str();
}

} // namespace duckdb
