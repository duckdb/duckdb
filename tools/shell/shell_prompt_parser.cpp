#include <iostream>
#include <string>
#include <unordered_map>
#include <functional>
#include <sstream>
#include <iomanip>
#include <cctype>
#include <cstdlib>

#include "shell_state.hpp"
#include "shell_prompt_parser.hpp"
using namespace duckdb_shell;

// ----------------------------
// Helper: convert escape sequences to actual characters
// Supports: \033, \x1b, \n, \t, \uXXXX
// ----------------------------
static std::string parse_escapes(const std::string &s) {
	std::string out;
	size_t i = 0;
	while (i < s.size()) {
		if (s[i] == '\\' && i + 1 < s.size()) {
			char c = s[i + 1];
			if (c == '0' && s.substr(i + 1, 3) == "033") {
				out += '\033';
				i += 4;
			} else if (c == 'x' && i + 3 < s.size()) {
				std::string hex = s.substr(i + 2, 2);
				out += static_cast<char>(std::stoi(hex, nullptr, 16));
				i += 4;
			} else if (c == 'e') {
				out += '\033';
				i += 2;
			} else if (c == 'n') {
				out += '\n';
				i += 2;
			} else if (c == 't') {
				out += '\t';
				i += 2;
			} else if (c == 'u' && i + 5 < s.size()) {
				std::string hex = s.substr(i + 2, 4);
				int codepoint = std::stoi(hex, nullptr, 16);
				// UTF-8 encoding
				if (codepoint <= 0x7F) {
					out += static_cast<char>(codepoint);
				} else if (codepoint <= 0x7FF) {
					out += static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F));
					out += static_cast<char>(0x80 | (codepoint & 0x3F));
				} else {
					out += static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F));
					out += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
					out += static_cast<char>(0x80 | (codepoint & 0x3F));
				}
				i += 6;
			} else if (c == '\\') {
				out += '\\';
				i += 2;
			} else {
				// Unknown escape, copy literally
				out += c;
				i += 2;
			}
		} else {
			out += s[i++];
		}
	}
	return out;
}

// ----------------------------
// Wrap ANSI sequences for Readline
// ----------------------------
static std::string rl_wrap_ansi(const std::string &s) {
	return "\001" + s + "\002";
}

static std::string expand_tokens(const std::string &fmt, ShellState &state) {
	std::string result;
	size_t i = 0;
	while (i < fmt.size()) {
		if (fmt[i] == '{') {
			size_t end = fmt.find('}', i);
			if (end != std::string::npos) {
				std::string statement = fmt.substr(i + 1, end - i - 1);
				result += state.ExecutePromptSQL(statement);
				i = end + 1;
			} else {
				result += fmt[i++];
			}
		} else {
			result += fmt[i++];
		}
	}
	return result;
}

// ----------------------------
// Wrap ANSI sequences automatically in the string for readline
// Finds sequences starting with ESC (\033) and ending with 'm'
// ----------------------------
static std::string wrap_all_ansi_for_readline(const std::string &s) {
	std::string out;
	size_t i = 0;
	while (i < s.size()) {
		if (s[i] == '\033') {
			size_t start = i;
			while (i < s.size() && s[i] != 'm')
				++i;
			if (i < s.size())
				++i; // include 'm'
			out += rl_wrap_ansi(s.substr(start, i - start));
		} else {
			out += s[i++];
		}
	}
	return out;
}

namespace duckdb_shell {

string get_prompt(const string &prompt, ShellState &state) {
	std::string parsed = parse_escapes(prompt);

	std::string expanded = expand_tokens(parsed, state);

#if HAVE_EDITLINE || !HAVE_LINENOISE
	return wrap_all_ansi_for_readline(expanded);
#else
	return expanded;
#endif
}
} // namespace duckdb_shell
