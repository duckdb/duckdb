#include "shell_prompt.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/client_data.hpp"

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

#if defined(_WIN32) || defined(WIN32)
string Prompt::HandleColor(const PromptComponent &component) {
	throw InvalidInputException("Prompt colors are not supported on Windows currently");
}

#else

string Prompt::HandleColor(const PromptComponent &component) {
	switch (component.type) {
	case PromptComponentType::SET_COLOR:
		return ShellHighlight::TerminalCode(component.color, PrintIntensity::STANDARD);
	case PromptComponentType::SET_INTENSITY:
		return ShellHighlight::TerminalCode(PrintColor::STANDARD, component.intensity);
	case PromptComponentType::RESET_COLOR:
		return ShellHighlight::ResetTerminalCode();
	default:
		throw InternalException("Invalid prompt color component");
	}
}
#endif

void Prompt::AddComponent(const string &bracket_type, const string &value) {
	PromptComponent component;
	if (bracket_type == "max_length") {
		if (value.empty()) {
			throw InvalidInputException("max_length requires a parameter");
		}
		max_length = StringUtil::ToUnsigned(value);
		return;
	} else if (bracket_type == "setting") {
		if (value.empty()) {
			throw InvalidInputException("setting requires a parameter");
		}
		vector<string> supported_settings {"current_database", "current_schema", "current_database_and_schema"};
		bool found = false;
		for (auto &entry : supported_settings) {
			if (value == entry) {
				found = true;
				break;
			}
		}
		if (!found) {
			throw InvalidInputException("unsupported setting \"%s\" for setting, supported values: %s", value,
			                            StringUtil::Join(supported_settings, ", "));
		}
		component.type = PromptComponentType::SETTING;
		component.literal = value;
	} else if (bracket_type == "sql") {
		if (value.empty()) {
			throw InvalidInputException("sql requires a parameter");
		}
		component.type = PromptComponentType::SQL;
		component.literal = value;
	} else if (bracket_type == "color") {
		if (value.empty()) {
			throw InvalidInputException("color requires a parameter");
		}
		if (value == "bold") {
			component.type = PromptComponentType::SET_INTENSITY;
			component.intensity = PrintIntensity::BOLD;
		} else if (value == "underline") {
			component.type = PromptComponentType::SET_INTENSITY;
			component.intensity = PrintIntensity::UNDERLINE;
		} else if (value == "reset") {
			component.type = PromptComponentType::RESET_COLOR;
		} else {
			// color value
			string error_msg;
			if (!ShellHighlight::TryGetPrintColor(value.c_str(), component.color, error_msg)) {
				throw InvalidInputException(error_msg);
			}
			component.type = PromptComponentType::SET_COLOR;
		}
	} else {
		throw InvalidInputException("Unknown bracket type %s", bracket_type);
	}
	components.push_back(std::move(component));
}

void Prompt::ParsePrompt(const string &prompt) {
	components.clear();
	max_length = optional_idx();
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

string Prompt::HandleSetting(ShellState &state, const PromptComponent &component) {
	if (!state.conn) {
		return component.literal == "current_schema" ? "main" : "memory";
	}
	auto &con = *state.conn;
	auto &current_db = duckdb::DatabaseManager::GetDefaultDatabase(*con.context);
	auto &current_schema = duckdb::ClientData::Get(*con.context).catalog_search_path->GetDefault().schema;
	;
	if (component.literal == "current_database") {
		return current_db;
	}
	if (component.literal == "current_schema") {
		return current_schema;
	}
	if (component.literal == "current_database_and_schema") {
		if (current_schema == "main") {
			return current_db;
		} else {
			return current_db + "." + current_schema;
		}
	}
	throw InternalException("Unsupported setting %s", component.literal);
}

string Prompt::HandleText(ShellState &state, const string &text, idx_t &length) {
	if (!max_length.IsValid()) {
		// no max length specified - just use the code
		return text;
	}
	if (length > max_length.GetIndex()) {
		// max length was already exceeded - skip rendering
		return string();
	}
	auto render_length = state.RenderLength(text.c_str());
	if (length + render_length <= max_length.GetIndex()) {
		// not exceeded - render entire string
		length += render_length;
		return text;
	}
	// length gets exceeded by this string - render whatever we can
	idx_t start_pos = 0;
	string truncated_text;
	for (idx_t i = 1; i <= text.size(); i++) {
		if (i < text.size() && !state.IsCharacter(text[i])) {
			// not a character - we cannot partially render at this position
			continue;
		}
		// this is a character - can we render the PREVIOUS character?
		auto prev_character = text.substr(start_pos, i - start_pos);
		auto char_length = state.RenderLength(prev_character);
		if (length + char_length > max_length.GetIndex()) {
			// we cannot - we are done!
			break;
		}
		// we can - render it and move to the next character
		truncated_text += prev_character;
		length += char_length;

		// set the start pos
		start_pos = i;
	}
	// add the final components
	truncated_text += "... D ";
	length += 6;
	return truncated_text;
}

string Prompt::GeneratePrompt(ShellState &state) {
	string prompt;
	idx_t length = 0;
	for (auto &component : components) {
		switch (component.type) {
		case PromptComponentType::LITERAL:
			prompt += HandleText(state, component.literal, length);
			break;
		case PromptComponentType::SQL: {
			auto query_result = EvaluateSQL(state, component.literal);
			prompt += HandleText(state, query_result, length);
			break;
		}
		case PromptComponentType::SET_COLOR:
		case PromptComponentType::SET_INTENSITY:
		case PromptComponentType::RESET_COLOR:
			prompt += HandleColor(component);
			break;
		case PromptComponentType::SETTING: {
			auto setting_val = HandleSetting(state, component);
			prompt += HandleText(state, setting_val, length);
			break;
		}
		default:
			throw InternalException("Invalid prompt component");
		}
	}
	return prompt;
}

void Prompt::PrintPrompt(ShellState &state, PrintOutput output) {
	ShellHighlight highlight(state);
	auto color = PrintColor::STANDARD;
	auto intensity = PrintIntensity::STANDARD;

	idx_t length = 0;
	for (auto &component : components) {
		switch (component.type) {
		case PromptComponentType::LITERAL:
			highlight.PrintText(HandleText(state, component.literal, length), output, color, intensity);
			break;
		case PromptComponentType::SQL: {
			auto result = EvaluateSQL(state, component.literal);
			highlight.PrintText(HandleText(state, result, length), output, color, intensity);
			break;
		}
		case PromptComponentType::SETTING: {
			auto result = HandleSetting(state, component);
			highlight.PrintText(HandleText(state, result, length), output, color, intensity);
			break;
		}
		case PromptComponentType::SET_COLOR: {
			color = component.color;
			break;
		}
		case PromptComponentType::SET_INTENSITY: {
			intensity = component.intensity;
			break;
		}
		case PromptComponentType::RESET_COLOR: {
			color = PrintColor::STANDARD;
			intensity = PrintIntensity::STANDARD;
			break;
		}
		default:
			throw InternalException("Invalid prompt component");
		}
	}
}

} // namespace duckdb_shell
