#include "shell_highlight.hpp"
#include "shell_state.hpp"
#include "duckdb/parser/parser.hpp"

#if defined(_WIN32) || defined(WIN32)
#include <windows.h>
#endif

namespace duckdb_shell {

struct HighlightElement {
	const char *name;
	PrintColor color;
	PrintIntensity intensity;
};

static HighlightElement highlight_elements[] = {{"error", PrintColor::RED, PrintIntensity::BOLD},
                                                {"keyword", PrintColor::GREEN, PrintIntensity::STANDARD},
                                                {"numeric_constant", PrintColor::YELLOW, PrintIntensity::STANDARD},
                                                {"string_constant", PrintColor::YELLOW, PrintIntensity::STANDARD},
                                                {"line_indicator", PrintColor::STANDARD, PrintIntensity::BOLD},
                                                {"column_name", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {"column_type", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {"numeric_value", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {"string_value", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {"temporal_value", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {"null_value", PrintColor::GRAY, PrintIntensity::STANDARD},
                                                {"footer", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {"layout", PrintColor::GRAY, PrintIntensity::STANDARD},
                                                {"startup_text", PrintColor::GRAY, PrintIntensity::STANDARD},
                                                {"startup_version", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {"none", PrintColor::STANDARD, PrintIntensity::STANDARD},
                                                {nullptr, PrintColor::STANDARD, PrintIntensity::STANDARD}};

struct HighlightColors {
	const char *name;
	PrintColor color;
};

static const HighlightColors highlight_colors[] = {{"standard", PrintColor::STANDARD}, {"red", PrintColor::RED},
                                                   {"yellow", PrintColor::YELLOW},     {"green", PrintColor::GREEN},
                                                   {"gray", PrintColor::GRAY},         {"blue", PrintColor::BLUE},
                                                   {"magenta", PrintColor::MAGENTA},   {"cyan", PrintColor::CYAN},
                                                   {"white", PrintColor::WHITE},       {nullptr, PrintColor::STANDARD}};

ShellHighlight::ShellHighlight(ShellState &state) : state(state) {
}

/*
** Output text to the console in a font that attracts extra attention.
*/
#ifdef _WIN32
void ShellHighlight::PrintText(const string &text, PrintOutput output, PrintColor color, PrintIntensity intensity) {
	HANDLE out = GetStdHandle(output == PrintOutput::STDOUT ? STD_OUTPUT_HANDLE : STD_ERROR_HANDLE);
	CONSOLE_SCREEN_BUFFER_INFO defaultScreenInfo;
	GetConsoleScreenBufferInfo(out, &defaultScreenInfo);
	WORD wAttributes = 0;

	switch (intensity) {
	case PrintIntensity::BOLD:
	case PrintIntensity::BOLD_UNDERLINE:
		wAttributes |= FOREGROUND_INTENSITY;
		break;
	default:
		break;
	}
	switch (color) {
	case PrintColor::RED:
		wAttributes |= FOREGROUND_RED;
		break;
	case PrintColor::GREEN:
		wAttributes |= FOREGROUND_GREEN;
		break;
	case PrintColor::BLUE:
		wAttributes |= FOREGROUND_BLUE;
		break;
	case PrintColor::YELLOW:
		wAttributes |= FOREGROUND_RED | FOREGROUND_GREEN;
		break;
	case PrintColor::GRAY:
		wAttributes |= FOREGROUND_RED | FOREGROUND_GREEN | FOREGROUND_BLUE;
		break;
	case PrintColor::MAGENTA:
		wAttributes |= FOREGROUND_BLUE | FOREGROUND_RED;
		break;
	case PrintColor::CYAN:
		wAttributes |= FOREGROUND_BLUE | FOREGROUND_GREEN;
		break;
	case PrintColor::WHITE:
		wAttributes |= FOREGROUND_BLUE | FOREGROUND_GREEN | FOREGROUND_RED;
		break;
	default:
		break;
	}
	if (wAttributes != 0) {
		SetConsoleTextAttribute(out, wAttributes);
	}

	state.Print(output, text);

	SetConsoleTextAttribute(out, defaultScreenInfo.wAttributes);
}
#else
void ShellHighlight::PrintText(const string &text, PrintOutput output, PrintColor color, PrintIntensity intensity) {
	const char *bold_prefix = "";
	const char *color_prefix = "";
	const char *suffix = "";
	switch (intensity) {
	case PrintIntensity::BOLD:
		bold_prefix = "\033[1m";
		break;
	case PrintIntensity::UNDERLINE:
		bold_prefix = "\033[4m";
		break;
	case PrintIntensity::BOLD_UNDERLINE:
		bold_prefix = "\033[1m\033[4m";
		break;
	default:
		break;
	}
	switch (color) {
	case PrintColor::RED:
		color_prefix = "\033[31m";
		break;
	case PrintColor::GREEN:
		color_prefix = "\033[32m";
		break;
	case PrintColor::YELLOW:
		color_prefix = "\033[33m";
		break;
	case PrintColor::GRAY:
		color_prefix = "\033[90m";
		break;
	case PrintColor::BLUE:
		color_prefix = "\033[34m";
		break;
	case PrintColor::MAGENTA:
		color_prefix = "\033[35m";
		break;
	case PrintColor::CYAN:
		color_prefix = "\033[36m";
		break;
	case PrintColor::WHITE:
		color_prefix = "\033[37m";
		break;
	default:
		break;
	}
	if (*color_prefix || *bold_prefix) {
		suffix = "\033[0m";
	}
	fprintf(output == PrintOutput::STDOUT ? state.out : stderr, "%s%s%s%s", bold_prefix, color_prefix, text.c_str(),
	        suffix);
}
#endif

void ShellHighlight::PrintText(const string &text, PrintOutput output, HighlightElementType type) {
	auto index = static_cast<uint32_t>(type);
	auto max_index = static_cast<uint32_t>(HighlightElementType::NONE);
	if (index > max_index) {
		index = max_index;
	}
	auto highlight_info = highlight_elements[index];
	PrintText(text, output, highlight_info.color, highlight_info.intensity);
}

void ShellHighlight::PrintError(string error_msg) {
	if (error_msg.empty()) {
		return;
	}
	vector<duckdb::SimplifiedToken> tokens;
	string error_type;
	auto error_location = duckdb::StringUtil::Find(error_msg, "Error: ");
	if (error_location.IsValid()) {
		error_type = error_msg.substr(0, error_location.GetIndex() + 6);
		error_msg = error_msg.substr(error_location.GetIndex() + 7);
	}
	try {
		tokens = duckdb::Parser::TokenizeError(error_msg);
	} catch (...) {
		// fallback
		state.Print(PrintOutput::STDERR, error_msg.c_str());
		state.Print(PrintOutput::STDERR, "\n");
		return;
	}
	if (!tokens.empty() && tokens[0].start > 0) {
		duckdb::SimplifiedToken new_token;
		new_token.type = duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
		new_token.start = 0;
		tokens.insert(tokens.begin(), new_token);
	}
	if (tokens.empty() && !error_msg.empty()) {
		duckdb::SimplifiedToken new_token;
		new_token.type = duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
		new_token.start = 0;
		tokens.push_back(new_token);
	}
	if (!error_type.empty()) {
		PrintText(error_type + "\n", PrintOutput::STDERR, HighlightElementType::ERROR_TOKEN);
	}
	for (idx_t i = 0; i < tokens.size(); i++) {
		HighlightElementType element_type = HighlightElementType::NONE;
		switch (tokens[i].type) {
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
			break;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_ERROR:
			element_type = HighlightElementType::ERROR_TOKEN;
			break;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
			element_type = HighlightElementType::NUMERIC_CONSTANT;
			break;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
			element_type = HighlightElementType::STRING_CONSTANT;
			break;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR:
			break;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
			element_type = HighlightElementType::KEYWORD;
			break;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
			element_type = HighlightElementType::LINE_INDICATOR;
			break;
		}
		idx_t start = tokens[i].start;
		idx_t end = i + 1 == tokens.size() ? error_msg.size() : tokens[i + 1].start;
		if (end - start > 0) {
			string error_print = error_msg.substr(tokens[i].start, end - start);
			PrintText(error_print, PrintOutput::STDERR, element_type);
		}
	}
	PrintText("\n", PrintOutput::STDERR, PrintColor::STANDARD, PrintIntensity::STANDARD);
}

bool ShellHighlight::SetColor(const char *element_type, const char *color, const char *intensity) {
	idx_t i;
	for (i = 0; highlight_elements[i].name; i++) {
		if (duckdb::StringUtil::CIEquals(element_type, highlight_elements[i].name)) {
			break;
		}
	}
	if (!highlight_elements[i].name) {
		// element not found
		string supported_options;
		for (i = 0; highlight_elements[i].name; i++) {
			if (!supported_options.empty()) {
				supported_options += ", ";
			}
			supported_options += highlight_elements[i].name;
		}
		state.Print(PrintOutput::STDERR, duckdb::StringUtil::Format("Unknown element '%s', supported options: %s\n",
		                                                            element_type, supported_options.c_str()));
		return false;
	}

	// found the element - parse the color
	idx_t c;
	for (c = 0; highlight_colors[c].name; c++) {
		if (duckdb::StringUtil::CIEquals(color, highlight_colors[c].name)) {
			break;
		}
	}
	if (!highlight_colors[c].name) {
		// color not found
		string supported_options;
		for (c = 0; highlight_colors[c].name; c++) {
			if (!supported_options.empty()) {
				supported_options += ", ";
			}
			supported_options += highlight_colors[c].name;
		}
		state.Print(PrintOutput::STDERR, duckdb::StringUtil::Format("Unknown color '%s', supported options: %s\n",
		                                                            color, supported_options.c_str()));
		return false;
	}
	highlight_elements[i].color = highlight_colors[c].color;
	highlight_elements[i].intensity = PrintIntensity::STANDARD;
	if (intensity) {
		if (duckdb::StringUtil::CIEquals(intensity, "standard")) {
			highlight_elements[i].intensity = PrintIntensity::STANDARD;
		} else if (duckdb::StringUtil::CIEquals(intensity, "bold")) {
			highlight_elements[i].intensity = PrintIntensity::BOLD;
		} else if (duckdb::StringUtil::CIEquals(intensity, "underline")) {
			highlight_elements[i].intensity = PrintIntensity::UNDERLINE;
		} else if (duckdb::StringUtil::CIEquals(intensity, "bold_underline")) {
			highlight_elements[i].intensity = PrintIntensity::BOLD_UNDERLINE;
		} else {
			state.Print(PrintOutput::STDERR,
			            duckdb::StringUtil::Format(
			                "Unknown intensity '%s', supported options: standard, bold, underline\n", intensity));
			return false;
		}
	}
	return true;
}

} // namespace duckdb_shell
