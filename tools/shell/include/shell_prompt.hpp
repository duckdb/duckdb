//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_prompt.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "shell_state.hpp"
#include "shell_highlight.hpp"

namespace duckdb_shell {

enum class PromptComponentType { LITERAL, SQL, SET_COLOR, SET_INTENSITY, RESET_COLOR };

struct PromptComponent {
	PromptComponentType type;
	string literal;
	PrintColor color;
	PrintIntensity intensity;
};

//{color:red}
//{color:blue}
//{color:bold}
//{color:reset}
//{max_length:32}
//{sql:SELECT ...}
struct Prompt {
public:
	void ParsePrompt(const string &prompt);
	string GeneratePrompt(ShellState &state);

private:
	vector<PromptComponent> components;

private:
	void AddLiteral(const string &str);
	void AddComponent(const string &bracket_type, const string &value);
	string EvaluateSQL(ShellState &state, const string &sql);
};

} // namespace duckdb_shell
