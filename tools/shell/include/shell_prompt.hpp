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

enum class PromptComponentType { LITERAL, SQL, SET_COLOR, SET_INTENSITY, RESET_COLOR, SETTING };

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
	virtual ~Prompt() = default;

	void ParsePrompt(const string &prompt);
	string GeneratePrompt(ShellState &state);
	void PrintPrompt(ShellState &state, PrintOutput output);

protected:
	vector<PromptComponent> components;
	optional_idx max_length;

protected:
	void AddLiteral(const string &str);
	void AddComponent(const string &bracket_type, const string &value);
	string EvaluateSQL(ShellState &state, const string &sql);
	string HandleColor(const PromptComponent &component);
	virtual bool ParseSetting(const string &bracket_type, const string &value);
	virtual string HandleSetting(ShellState &state, const PromptComponent &component);
	virtual vector<string> GetSupportedSettings();
	string HandleText(ShellState &state, const string &text, idx_t &length);
	string ExecuteSQL(ShellState &state, const string &query);
	virtual duckdb::Connection &GetConnection(ShellState &state);
};

} // namespace duckdb_shell
