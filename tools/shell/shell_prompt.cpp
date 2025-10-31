#include "shell_prompt.hpp"

namespace duckdb_shell {

enum class PromptParseState { STANDARD, PARSE_BRACKET_TYPE, PARSE_BRACKET_CONTENT, ESCAPED };

void Prompt::AddLiteral(const string &str) {
	if (components.empty() || components.back().type != PromptComponentType::LITERAL) {
		PromptComponent component;
		component.type = PromptComponentType::LITERAL;
		components.push_back(std::move(component));
	}
	components.back().literal += str;
}

void Prompt::AddComponent(const string &bracket_type, const string &value) {
	PromptComponent component;
	if (bracket_type == "sql") {
		if (value.empty()) {
			throw InvalidInputException("sql requires a parameter", bracket_type);
		}
		component.type = PromptComponentType::SQL;
		component.literal = value;
	} else {
		throw InvalidInputException("Unknown bracket type %s", bracket_type);
	}
	components.push_back(std::move(component));
}

void Prompt::ParsePrompt(const string &prompt) {
	components.clear();
	PromptParseState parse_state = PromptParseState::STANDARD;
	PromptParseState prev_state = parse_state;
	string bracket_type;
	string literal;
	for (auto c : prompt) {
		switch (parse_state) {
		case PromptParseState::STANDARD:
			switch (c) {
			case '\\':
				// escape - add next character literally
				prev_state = parse_state;
				parse_state = PromptParseState::ESCAPED;
				break;
			case '{':
				// found a bracket - move to parsing it
				if (!literal.empty()) {
					AddLiteral(literal);
					literal.clear();
				}
				parse_state = PromptParseState::PARSE_BRACKET_TYPE;
				break;
			default:
				literal += c;
				break;
			}
			break;
		case PromptParseState::ESCAPED:
			// escaped character - revert to original state
			literal += c;
			parse_state = prev_state;
			break;
		case PromptParseState::PARSE_BRACKET_TYPE:
			switch (c) {
			case '}': {
				// closing bracket - this is a bracket type WITHOUT value
				string value;
				AddComponent(literal, value);
				literal.clear();
				parse_state = PromptParseState::STANDARD;
				break;
			}
			case ':':
				// colon - this is the name of the bracket type
				bracket_type = std::move(literal);
				literal.clear();
				// now move to parsing the value
				parse_state = PromptParseState::PARSE_BRACKET_CONTENT;
				break;
			case '\\':
				// escape - add next character literally
				prev_state = parse_state;
				parse_state = PromptParseState::ESCAPED;
				break;
			default:
				literal += c;
				break;
			}
			break;
		case PromptParseState::PARSE_BRACKET_CONTENT:
			switch (c) {
			case '}':
				// closing bracket - we have terminated the bracket - add the component
				AddComponent(bracket_type, literal);
				bracket_type.clear();
				literal.clear();
				parse_state = PromptParseState::STANDARD;
				break;
			case '\\':
				// escape - add next character literally
				prev_state = parse_state;
				parse_state = PromptParseState::ESCAPED;
				break;
			default:
				literal += c;
				break;
			}
			break;
		default:
			throw InternalException("Invalid prompt state");
		}
	}
	if (parse_state != PromptParseState::STANDARD) {
		throw InvalidInputException("Failed to parse prompt \"%s\" - unterminated bracket or escape", prompt);
	}
	if (!literal.empty()) {
		AddLiteral(literal);
		literal.clear();
	}
}

string Prompt::EvaluateSQL(ShellState &state, const string &sql) {
	state.OpenDB();
	auto &con = *state.conn;
	auto result = con.Query(sql);
	if (result->HasError()) {
		return "#ERROR#:" + result->GetError();
	}
	auto &collection = result->Collection();
	if (collection.Count() > 1) {
		return "#TOO MANY ROWS#";
	}
	if (collection.ColumnCount() != 1) {
		return "#TOO MANY COLUMNS#";
	}
	for (auto &row : collection.Rows()) {
		return row.GetValue(0).ToString();
	}
	return "#EMPTY#";
}

string Prompt::GeneratePrompt(ShellState &state) {
	string prompt;
	for (auto &component : components) {
		switch (component.type) {
		case PromptComponentType::LITERAL:
			prompt += component.literal;
			break;
		case PromptComponentType::SQL:
			prompt += EvaluateSQL(state, component.literal);
			break;
		default:
			throw InternalException("Invalid prompt component");
		}
	}
	return prompt;
}

} // namespace duckdb_shell
