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

enum class PromptComponentType { LITERAL, SQL, SET_COLOR, SET_INTENSITY, RESET_COLOR, SET_COLOR_RGB, SETTING };

struct PromptComponent {
	PromptComponentType type;
	string literal;
	PrintColor color;
	PrintIntensity intensity;
};

// Supports dynamic prompts
// Example prompt:
// {max_length:40}{color:green}{color:bold}{setting:current_database_and_schema}{color:reset} D "
struct Prompt {
public:
	void ParsePrompt(const string &prompt);
	string GeneratePrompt(ShellState &state);
	void PrintPrompt(ShellState &state, PrintOutput output);

private:
	vector<PromptComponent> components;
	optional_idx max_length;

private:
	void AddLiteral(const string &str);
	void AddComponent(const string &bracket_type, const string &value);
	string EvaluateSQL(ShellState &state, const string &sql);
	string HandleColor(const PromptComponent &component);
	string HandleSetting(ShellState &state, const PromptComponent &component);
	string HandleText(ShellState &state, const string &text, idx_t &length);
};

} // namespace duckdb_shell
