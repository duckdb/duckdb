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
	CTRL_J = 10,    /* Ctrl+j*/
	CTRL_K = 11,    /* Ctrl+k */
	CTRL_L = 12,    /* Ctrl+l */
	ENTER = 13,     /* Enter */
	CTRL_N = 14,    /* Ctrl-n */
	CTRL_O = 15,    /* Ctrl-O */
	CTRL_P = 16,    /* Ctrl-p */
	CTRL_R = 18,    /* Ctrl-r */
	CTRL_S = 19,    /* Ctrl-s */
	CTRL_T = 20,    /* Ctrl-t */
	CTRL_U = 21,    /* Ctrl+u */
	CTRL_W = 23,    /* Ctrl+w */
	CTRL_X = 24,    /* Ctrl+x */
	CTRL_Y = 25,    /* Ctrl+y */
	CTRL_Z = 26,    /* Ctrl+z */
	ESC = 27,       /* Escape */
	BACKSPACE = 127 /* Backspace */
};

enum class EscapeSequence {
	INVALID = 0,
	UNKNOWN = 1,
	CTRL_MOVE_BACKWARDS,
	CTRL_MOVE_FORWARDS,
	HOME,
	END,
	UP,
	DOWN,
	RIGHT,
	LEFT,
	DELETE,
	SHIFT_TAB,
	ESCAPE,
	ALT_A,
	ALT_B,
	ALT_C,
	ALT_D,
	ALT_E,
	ALT_F,
	ALT_G,
	ALT_H,
	ALT_I,
	ALT_J,
	ALT_K,
	ALT_L,
	ALT_M,
	ALT_N,
	ALT_O,
	ALT_P,
	ALT_Q,
	ALT_R,
	ALT_S,
	ALT_T,
	ALT_U,
	ALT_V,
	ALT_W,
	ALT_X,
	ALT_Y,
	ALT_Z,
	ALT_BACKSPACE,
	ALT_LEFT_ARROW,
	ALT_RIGHT_ARROW,
	ALT_BACKSLASH,
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

	static EscapeSequence ReadEscapeSequence(int ifd);

private:
	static TerminalSize TryMeasureTerminalSize();
	static TerminalSize GetCursorPosition();
	static idx_t ReadEscapeSequence(int ifd, char sequence[]);
};

} // namespace duckdb
