//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_highlight.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "shell_state.hpp"

namespace duckdb_shell {

enum class PrintColor { STANDARD, RED, YELLOW, GREEN, GRAY, BLUE, MAGENTA, CYAN, WHITE };

enum class PrintIntensity { STANDARD, BOLD, UNDERLINE, BOLD_UNDERLINE };

enum class HighlightElementType : uint32_t {
	ERROR_TOKEN = 0,
	KEYWORD,
	NUMERIC_CONSTANT,
	STRING_CONSTANT,
	LINE_INDICATOR,
	COLUMN_NAME,
	COLUMN_TYPE,
	NUMERIC_VALUE,
	STRING_VALUE,
	TEMPORAL_VALUE,
	NULL_VALUE,
	FOOTER,
	LAYOUT,
	STARTUP_TEXT,
	STARTUP_VERSION,
	NONE
};

struct ShellHighlight {
	explicit ShellHighlight(ShellState &state);

	void PrintText(const string &text, PrintOutput output, PrintColor color, PrintIntensity intensity);
	void PrintText(const string &text, PrintOutput output, HighlightElementType type);

	void PrintError(string error_msg);

	bool SetColor(const char *element_type, const char *color, const char *intensity);

public:
	ShellState &state;
};

} // namespace duckdb_shell
