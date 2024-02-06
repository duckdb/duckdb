//===----------------------------------------------------------------------===//
//                         DuckDB
//
// terminal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum KEY_ACTION {
	KEY_NULL = 0,   /* NULL */
	CTRL_A = 1,     /* Ctrl+a */
	CTRL_B = 2,     /* Ctrl-b */
	CTRL_C = 3,     /* Ctrl-c */
	CTRL_D = 4,     /* Ctrl-d */
	CTRL_E = 5,     /* Ctrl-e */
	CTRL_F = 6,     /* Ctrl-f */
	CTRL_G = 7,     /* Ctrl-g */
	CTRL_H = 8,     /* Ctrl-h */
	TAB = 9,        /* Tab */
	CTRL_K = 11,    /* Ctrl+k */
	CTRL_L = 12,    /* Ctrl+l */
	ENTER = 13,     /* Enter */
	CTRL_N = 14,    /* Ctrl-n */
	CTRL_P = 16,    /* Ctrl-p */
	CTRL_R = 18,    /* Ctrl-r */
	CTRL_T = 20,    /* Ctrl-t */
	CTRL_U = 21,    /* Ctrl+u */
	CTRL_W = 23,    /* Ctrl+w */
	CTRL_Z = 26,    /* Ctrl+z */
	ESC = 27,       /* Escape */
	BACKSPACE = 127 /* Backspace */
};

struct TerminalSize {
	int ws_col = 0;
	int ws_row = 0;
};

class Terminal {
public:
	static int IsUnsupportedTerm();
	static int EnableRawMode();
	static void DisableRawMode();
	static bool IsMultiline();
	static void SetMultiLine(int ml);

	static void ClearScreen();
	static void Beep();

	static bool IsAtty();
	static int HasMoreData(int fd);
	static TerminalSize GetTerminalSize();

	static char *EditNoTTY();
	static int EditRaw(char *buf, size_t buflen, const char *prompt);

private:
	static TerminalSize TryMeasureTerminalSize();
	static TerminalSize GetCursorPosition();
};

} // namespace duckdb
