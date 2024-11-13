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

enum class PrintOutput { STDOUT, STDERR };

enum class PrintColor { STANDARD, RED, YELLOW, GREEN, GRAY, BLUE, MAGENTA, CYAN, WHITE };

enum class PrintIntensity { STANDARD, BOLD, UNDERLINE };

enum class HighlightElementType : uint32_t {
	ERROR = 0,
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
	NONE
};

struct ShellHighlight {
	static void PrintText(const string &text, PrintOutput output, PrintColor color, PrintIntensity intensity);
	static void PrintText(const string &text, PrintOutput output, HighlightElementType type);

	static void PrintError(string error_msg);

	static bool SetColor(ShellState &state, const char *element_type, const char *color, const char *intensity);
};

} // namespace duckdb_shell
