/* linenoise.c -- guerrilla line editing library against the idea that a
 * line editing lib needs to be 20,000 lines of C code.
 *
 * You can find the latest source code at:
 *
 *   http://github.com/antirez/linenoise
 *
 * Does a number of crazy assumptions that happen to be true in 99.9999% of
 * the 2010 UNIX computers around.
 *
 * ------------------------------------------------------------------------
 *
 * Copyright (c) 2010-2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2013, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *  *  Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  *  Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * ------------------------------------------------------------------------
 *
 * References:
 * - http://invisible-island.net/xterm/ctlseqs/ctlseqs.html
 * - http://www.3waylabs.com/nw/WWW/products/wizcon/vt220.html
 *
 * Todo list:
 * - Filter bogus Ctrl+<char> combinations.
 * - Win32 support
 *
 * Bloat:
 * - History search like Ctrl+r in readline?
 *
 * List of escape sequences used by this program, we do everything just
 * with three sequences. In order to be so cheap we may have some
 * flickering effect with some slow terminal, but the lesser sequences
 * the more compatible.
 *
 * EL (Erase Line)
 *    Sequence: ESC [ n K
 *    Effect: if n is 0 or missing, clear from cursor to end of line
 *    Effect: if n is 1, clear from beginning of line to cursor
 *    Effect: if n is 2, clear entire line
 *
 * CUF (CUrsor Forward)
 *    Sequence: ESC [ n C
 *    Effect: moves cursor forward n chars
 *
 * CUB (CUrsor Backward)
 *    Sequence: ESC [ n D
 *    Effect: moves cursor backward n chars
 *
 * The following is used to get the terminal width if getting
 * the width with the TIOCGWINSZ ioctl fails
 *
 * DSR (Device Status Report)
 *    Sequence: ESC [ 6 n
 *    Effect: reports the current cusor position as ESC [ n ; m R
 *            where n is the row and m is the column
 *
 * When multi line mode is enabled, we also use an additional escape
 * sequence. However multi line editing is disabled by default.
 *
 * CUU (Cursor Up)
 *    Sequence: ESC [ n A
 *    Effect: moves cursor up of n chars.
 *
 * CUD (Cursor Down)
 *    Sequence: ESC [ n B
 *    Effect: moves cursor down of n chars.
 *
 * When linenoiseClearScreen() is called, two additional escape sequences
 * are used in order to clear the screen and position the cursor at home
 * position.
 *
 * CUP (Cursor position)
 *    Sequence: ESC [ H
 *    Effect: moves the cursor to upper left corner
 *
 * ED (Erase display)
 *    Sequence: ESC [ 2 J
 *    Effect: clear the whole screen
 *
 */

#include <termios.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include "linenoise.h"
#include "utf8proc_wrapper.hpp"
#include <unordered_set>
#include <vector>
#ifdef __MVS__
#include <strings.h>
#include <sys/time.h>
#endif

#if defined(_WIN32) || defined(__WIN32__) || defined(WIN32)
// disable highlighting on windows (for now?)
#define DISABLE_HIGHLIGHT
#endif

#define LINENOISE_DEFAULT_HISTORY_MAX_LEN 100
#define LINENOISE_MAX_LINE                4096
static const char *unsupported_term[] = {"dumb", "cons25", "emacs", NULL};
static linenoiseCompletionCallback *completionCallback = NULL;
static linenoiseHintsCallback *hintsCallback = NULL;
static linenoiseFreeHintsCallback *freeHintsCallback = NULL;

static struct termios orig_termios; /* In order to restore at exit.*/
static int rawmode = 0;             /* For atexit() function to check if restore is needed*/
static int mlmode = 0;              /* Multi line mode. Default is single line. */
static int atexit_registered = 0;   /* Register atexit just 1 time. */
static int history_max_len = LINENOISE_DEFAULT_HISTORY_MAX_LEN;
static int history_len = 0;
static char **history = NULL;
static char *history_file = NULL;
#ifndef DISABLE_HIGHLIGHT
#include <string>

static int enableHighlighting = 1;
struct Color {
	const char *color_name;
	const char *highlight;
};
static Color terminal_colors[] = {{"red", "\033[31m"},           {"green", "\033[32m"},
                                  {"yellow", "\033[33m"},        {"blue", "\033[34m"},
                                  {"magenta", "\033[35m"},       {"cyan", "\033[36m"},
                                  {"white", "\033[37m"},         {"brightblack", "\033[90m"},
                                  {"brightred", "\033[91m"},     {"brightgreen", "\033[92m"},
                                  {"brightyellow", "\033[93m"},  {"brightblue", "\033[94m"},
                                  {"brightmagenta", "\033[95m"}, {"brightcyan", "\033[96m"},
                                  {"brightwhite", "\033[97m"},   {nullptr, nullptr}};
static std::string bold = "\033[1m";
static std::string underline = "\033[4m";
static std::string keyword = "\033[32m\033[1m";
static std::string constant = "\033[33m";
static std::string reset = "\033[00m";
#endif

struct searchMatch {
	size_t history_index;
	size_t match_start;
	size_t match_end;
};

/* The linenoiseState structure represents the state during line editing.
 * We pass this state to functions implementing specific editing
 * functionalities. */
struct linenoiseState {
	int ifd;                                 /* Terminal stdin file descriptor. */
	int ofd;                                 /* Terminal stdout file descriptor. */
	char *buf;                               /* Edited line buffer. */
	size_t buflen;                           /* Edited line buffer size. */
	const char *prompt;                      /* Prompt to display. */
	size_t plen;                             /* Prompt length. */
	size_t pos;                              /* Current cursor position. */
	size_t oldpos;                           /* Previous refresh cursor position. */
	size_t len;                              /* Current edited line length. */
	size_t cols;                             /* Number of columns in terminal. */
	size_t maxrows;                          /* Maximum num of rows used so far (multiline mode) */
	int history_index;                       /* The history index we are currently editing. */
	bool search;                             /* Whether or not we are searching our history */
	std::string search_buf;                  //! The search buffer
	std::vector<searchMatch> search_matches; //! The set of search matches in our history
	size_t search_index;                     //! The current match index
};

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
	ESC = 27,       /* Escape */
	BACKSPACE = 127 /* Backspace */
};

static void linenoiseAtExit(void);
int linenoiseHistoryAdd(const char *line);
static void refreshLine(struct linenoiseState *l);

/* Debugging macro. */
#if 0
FILE *lndebug_fp = NULL;
#define lndebug(...)                                                                                                   \
	do {                                                                                                               \
		if (lndebug_fp == NULL) {                                                                                      \
			lndebug_fp = fopen("/tmp/lndebug.txt", "a");                                                               \
		}                                                                                                              \
		fprintf(lndebug_fp, ", " __VA_ARGS__);                                                                         \
		fflush(lndebug_fp);                                                                                            \
	} while (0)
#else
#define lndebug(fmt, ...)
#endif

/* ======================= Low level terminal handling ====================== */

/* Set if to use or not the multi line mode. */
void linenoiseSetMultiLine(int ml) {
	mlmode = ml;
	if (ml) {
		keyword = "\033[32m";
	}
}

/* Return true if the terminal name is in the list of terminals we know are
 * not able to understand basic escape sequences. */
static int isUnsupportedTerm(void) {
	char *term = getenv("TERM");
	int j;

	if (term == NULL)
		return 0;
	for (j = 0; unsupported_term[j]; j++)
		if (!strcasecmp(term, unsupported_term[j]))
			return 1;
	return 0;
}

/* Raw mode: 1960 magic shit. */
static int enableRawMode(int fd) {
	struct termios raw;

	if (!isatty(STDIN_FILENO))
		goto fatal;
	if (!atexit_registered) {
		atexit(linenoiseAtExit);
		atexit_registered = 1;
	}
	if (tcgetattr(fd, &orig_termios) == -1)
		goto fatal;

	raw = orig_termios; /* modify the original mode */
	/* input modes: no break, no CR to NL, no parity check, no strip char,
	 * no start/stop output control. */
	raw.c_iflag &= ~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
	/* output modes - disable post processing */
	raw.c_oflag &= ~(OPOST);
	/* control modes - set 8 bit chars */
	raw.c_cflag |= (CS8);
	/* local modes - choing off, canonical off, no extended functions,
	 * no signal chars (^Z,^C) */
	raw.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
	/* control chars - set return condition: min number of bytes and timer.
	 * We want read to return every single byte, without timeout. */
	raw.c_cc[VMIN] = 1;
	raw.c_cc[VTIME] = 0; /* 1 byte, no timer */

	/* put terminal in raw mode after flushing */
	if (tcsetattr(fd, TCSADRAIN, &raw) < 0)
		goto fatal;
	rawmode = 1;
	return 0;

fatal:
	errno = ENOTTY;
	return -1;
}

static void disableRawMode(int fd) {
	/* Don't even check the return value as it's too late. */
	if (rawmode && tcsetattr(fd, TCSADRAIN, &orig_termios) != -1)
		rawmode = 0;
}

/* Use the ESC [6n escape sequence to query the horizontal cursor position
 * and return it. On error -1 is returned, on success the position of the
 * cursor. */
static int getCursorPosition(int ifd, int ofd) {
	char buf[32];
	int cols, rows;
	unsigned int i = 0;

	/* Report cursor location */
	if (write(ofd, "\x1b[6n", 4) != 4)
		return -1;

	/* Read the response: ESC [ rows ; cols R */
	while (i < sizeof(buf) - 1) {
		if (read(ifd, buf + i, 1) != 1)
			break;
		if (buf[i] == 'R')
			break;
		i++;
	}
	buf[i] = '\0';

	/* Parse it. */
	if (buf[0] != ESC || buf[1] != '[')
		return -1;
	if (sscanf(buf + 2, "%d;%d", &rows, &cols) != 2)
		return -1;
	return cols;
}

/* Try to get the number of columns in the current terminal, or assume 80
 * if it fails. */
static int getColumns(int ifd, int ofd) {
	struct winsize ws;

	if (ioctl(1, TIOCGWINSZ, &ws) == -1 || ws.ws_col == 0) {
		/* ioctl() failed. Try to query the terminal itself. */
		int start, cols;

		/* Get the initial position so we can restore it later. */
		start = getCursorPosition(ifd, ofd);
		if (start == -1)
			goto failed;

		/* Go to right margin and get position. */
		if (write(ofd, "\x1b[999C", 6) != 6)
			goto failed;
		cols = getCursorPosition(ifd, ofd);
		if (cols == -1)
			goto failed;

		/* Restore position. */
		if (cols > start) {
			char seq[32];
			snprintf(seq, 32, "\x1b[%dD", cols - start);
			if (write(ofd, seq, strlen(seq)) == -1) {
				/* Can't recover... */
			}
		}
		return cols;
	} else {
		return ws.ws_col;
	}

failed:
	return 80;
}

/* Clear the screen. Used to handle ctrl+l */
void linenoiseClearScreen(void) {
	if (write(STDOUT_FILENO, "\x1b[H\x1b[2J", 7) <= 0) {
		/* nothing to do, just to avoid warning. */
	}
}

/* Beep, used for completion when there is nothing to complete or when all
 * the choices were already shown. */
static void linenoiseBeep(void) {
	fprintf(stderr, "\x7");
	fflush(stderr);
}

/* ============================== Completion ================================ */

/* Free a list of completion option populated by linenoiseAddCompletion(). */
static void freeCompletions(linenoiseCompletions *lc) {
	size_t i;
	for (i = 0; i < lc->len; i++)
		free(lc->cvec[i]);
	if (lc->cvec != NULL)
		free(lc->cvec);
}

/* This is an helper function for linenoiseEdit() and is called when the
 * user types the <tab> key in order to complete the string currently in the
 * input.
 *
 * The state of the editing is encapsulated into the pointed linenoiseState
 * structure as described in the structure definition. */
static int completeLine(struct linenoiseState *ls) {
	linenoiseCompletions lc = {0, NULL};
	int nread, nwritten;
	char c = 0;

	completionCallback(ls->buf, &lc);
	if (lc.len == 0) {
		linenoiseBeep();
	} else {
		size_t stop = 0, i = 0;

		while (!stop) {
			/* Show completion or original buffer */
			if (i < lc.len) {
				struct linenoiseState saved = *ls;

				ls->len = ls->pos = strlen(lc.cvec[i]);
				ls->buf = lc.cvec[i];
				refreshLine(ls);
				ls->len = saved.len;
				ls->pos = saved.pos;
				ls->buf = saved.buf;
			} else {
				refreshLine(ls);
			}

			nread = read(ls->ifd, &c, 1);
			if (nread <= 0) {
				freeCompletions(&lc);
				return -1;
			}

			switch (c) {
			case 9: /* tab */
				i = (i + 1) % (lc.len + 1);
				if (i == lc.len)
					linenoiseBeep();
				break;
			case 27: /* escape */
				/* Re-show original buffer */
				if (i < lc.len)
					refreshLine(ls);
				stop = 1;
				break;
			default:
				/* Update buffer and return */
				if (i < lc.len) {
					nwritten = snprintf(ls->buf, ls->buflen, "%s", lc.cvec[i]);
					ls->len = ls->pos = nwritten;
				}
				stop = 1;
				break;
			}
		}
	}

	freeCompletions(&lc);
	return c; /* Return last read character */
}

/* Register a callback function to be called for tab-completion. */
void linenoiseSetCompletionCallback(linenoiseCompletionCallback *fn) {
	completionCallback = fn;
}

/* Register a hits function to be called to show hits to the user at the
 * right of the prompt. */
void linenoiseSetHintsCallback(linenoiseHintsCallback *fn) {
	hintsCallback = fn;
}

/* Register a function to free the hints returned by the hints callback
 * registered with linenoiseSetHintsCallback(). */
void linenoiseSetFreeHintsCallback(linenoiseFreeHintsCallback *fn) {
	freeHintsCallback = fn;
}

/* This function is used by the callback function registered by the user
 * in order to add completion options given the input string when the
 * user typed <tab>. See the example.c source code for a very easy to
 * understand example. */
void linenoiseAddCompletion(linenoiseCompletions *lc, const char *str) {
	size_t len = strlen(str);
	char *copy, **cvec;

	copy = (char *)malloc(len + 1);
	if (copy == NULL)
		return;
	memcpy(copy, str, len + 1);
	cvec = (char **)realloc(lc->cvec, sizeof(char *) * (lc->len + 1));
	if (cvec == NULL) {
		free(copy);
		return;
	}
	lc->cvec = cvec;
	lc->cvec[lc->len++] = copy;
}

/* =========================== Line editing ================================= */

/* We define a very simple "append buffer" structure, that is an heap
 * allocated string where we can append to. This is useful in order to
 * write all the escape sequences in a buffer and flush them to the standard
 * output in a single call, to avoid flickering effects. */
struct abuf {
	char *b;
	int len;
};

static void abInit(struct abuf *ab) {
	ab->b = NULL;
	ab->len = 0;
}

static void abAppend(struct abuf *ab, const char *s, int len) {
	char *new_entry = (char *)realloc(ab->b, ab->len + len);

	if (new_entry == NULL)
		return;
	memcpy(new_entry + ab->len, s, len);
	ab->b = new_entry;
	ab->len += len;
}

static void abFree(struct abuf *ab) {
	free(ab->b);
}

/* Helper of refreshSingleLine() and refreshMultiLine() to show hints
 * to the right of the prompt. */
void refreshShowHints(struct abuf *ab, struct linenoiseState *l, int plen) {
	char seq[64];
	if (hintsCallback && plen + l->len < l->cols) {
		int color = -1, bold = 0;
		char *hint = hintsCallback(l->buf, &color, &bold);
		if (hint) {
			int hintlen = strlen(hint);
			int hintmaxlen = l->cols - (plen + l->len);
			if (hintlen > hintmaxlen)
				hintlen = hintmaxlen;
			if (bold == 1 && color == -1)
				color = 37;
			if (color != -1 || bold != 0)
				snprintf(seq, 64, "\033[%d;%d;49m", bold, color);
			else
				seq[0] = '\0';
			abAppend(ab, seq, strlen(seq));
			abAppend(ab, hint, hintlen);
			if (color != -1 || bold != 0)
				abAppend(ab, "\033[0m", 4);
			/* Call the function to free the hint returned. */
			if (freeHintsCallback)
				freeHintsCallback(hint);
		}
	}
}

size_t linenoiseComputeRenderWidth(const char *buf, size_t len) {
	// utf8 in prompt, get render width
	size_t cpos = 0;
	size_t render_width = 0;
	int sz;
	while (cpos < len) {
		if (duckdb::Utf8Proc::UTF8ToCodepoint(buf + cpos, sz) < 0) {
			cpos++;
			render_width++;
			continue;
		}

		size_t char_render_width = duckdb::Utf8Proc::RenderWidth(buf, len, cpos);
		cpos = duckdb::Utf8Proc::NextGraphemeCluster(buf, len, cpos);
		render_width += char_render_width;
	}
	return render_width;
}

int linenoiseGetRenderPosition(const char *buf, size_t len, int max_width, int *n) {
	if (duckdb::Utf8Proc::IsValid(buf, len)) {
		// utf8 in prompt, get render width
		size_t cpos = 0;
		size_t render_width = 0;
		while (cpos < len) {
			size_t char_render_width = duckdb::Utf8Proc::RenderWidth(buf, len, cpos);
			if (int(render_width + char_render_width) > max_width) {
				*n = render_width;
				return cpos;
			}
			cpos = duckdb::Utf8Proc::NextGraphemeCluster(buf, len, cpos);
			render_width += char_render_width;
		}
		*n = render_width;
		return len;
	} else {
		// invalid utf8, return -1
		return -1;
	}
}

#ifndef DISABLE_HIGHLIGHT
const char *getColorOption(const char *option) {
	size_t index = 0;
	while (terminal_colors[index].color_name) {
		if (strcmp(terminal_colors[index].color_name, option) == 0) {
			return terminal_colors[index].highlight;
		}
		index++;
	}
	return nullptr;
}
#endif

int linenoiseParseOption(const char **azArg, int nArg, const char **out_error) {
#ifndef DISABLE_HIGHLIGHT
	if (strcmp(azArg[0], "highlight") == 0) {
		if (nArg == 2) {
			if (strcmp(azArg[1], "off") == 0 || strcmp(azArg[1], "0") == 0) {
				enableHighlighting = 0;
				return 1;
			} else if (strcmp(azArg[1], "on") == 0 || strcmp(azArg[1], "1") == 0) {
				enableHighlighting = 1;
				return 1;
			}
		}
		*out_error = "Expected usage: .highlight [off|on]";
		return 1;
	} else if (strcmp(azArg[0], "keyword") == 0) {
		if (nArg == 2) {
			const char *option = getColorOption(azArg[1]);
			if (option) {
				keyword = option;
				return 1;
			}
		}
		*out_error = "Expected usage: .keyword "
		             "[red|green|yellow|blue|magenta|cyan|white|brightblack|brightred|brightgreen|brightyellow|"
		             "brightblue|brightmagenta|brightcyan|brightwhite]";
		return 1;
	} else if (strcmp(azArg[0], "constant") == 0) {
		if (nArg == 2) {
			const char *option = getColorOption(azArg[1]);
			if (option) {
				constant = option;
				return 1;
			}
		}
		*out_error = "Expected usage: .constant "
		             "[red|green|yellow|blue|magenta|cyan|white|brightblack|brightred|brightgreen|brightyellow|"
		             "brightblue|brightmagenta|brightcyan|brightwhite]";
		return 1;
	} else if (strcmp(azArg[0], "keywordcode") == 0) {
		if (nArg == 2) {
			keyword = azArg[1];
			return 1;
		}
		*out_error = "Expected usage: .keywordcode [terminal_code]";
		return 1;
	} else if (strcmp(azArg[0], "constantcode") == 0) {
		if (nArg == 2) {
			constant = azArg[1];
			return 1;
		}
		*out_error = "Expected usage: .constantcode [terminal_code]";
		return 1;
	} else if (strcmp(azArg[0], "multiline") == 0) {
		linenoiseSetMultiLine(1);
		return 1;
	} else if (strcmp(azArg[0], "singleline") == 0) {
		linenoiseSetMultiLine(0);
		return 1;
	}
#endif
	return 0;
}

#ifndef DISABLE_HIGHLIGHT
#include <sstream>
#include "duckdb/parser/parser.hpp"

struct highlightToken {
	duckdb::SimplifiedTokenType type;
	size_t start = 0;
	bool search_match = false;
};

std::string highlightText(char *buf, size_t len, size_t start_pos, size_t end_pos, searchMatch *match = nullptr) {
	std::string sql(buf, len);
	auto parseTokens = duckdb::Parser::Tokenize(sql);
	std::stringstream ss;
	std::vector<highlightToken> tokens;

	for (auto &token : parseTokens) {
		highlightToken new_token;
		new_token.type = token.type;
		new_token.start = token.start;
		tokens.push_back(new_token);
	}

	if (!tokens.empty() && tokens[0].start > 0) {
		highlightToken new_token;
		new_token.type = duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
		new_token.start = 0;
		tokens.insert(tokens.begin(), new_token);
	}
	if (tokens.empty() && sql.size() > 0) {
		highlightToken new_token;
		new_token.type = duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
		new_token.start = 0;
		tokens.push_back(new_token);
	}
	if (match) {
		// we have a search match - insert it into the token list
		// we want to insert a search token with start = match_start, end = match_end
		// first figure out which token type we would have at match_end (if any)
		for (size_t i = 0; i + 1 < tokens.size(); i++) {
			if (tokens[i].start <= match->match_start && tokens[i + 1].start >= match->match_start) {
				// this token begins after the search position, insert the token here
				size_t token_position = i + 1;
				duckdb::SimplifiedTokenType end_type = tokens[i].type;
				if (tokens[i].start == match->match_start) {
					// exact start: only set the search match
					tokens[i].search_match = true;
				} else {
					// non-exact start: add a new token
					highlightToken search_token;
					search_token.type = tokens[i].type;
					search_token.start = match->match_start;
					search_token.search_match = true;
					tokens.insert(tokens.begin() + token_position, search_token);
					token_position++;
				}

				// move forwards
				while (token_position < tokens.size() && tokens[token_position].start < match->match_end) {
					// this token is
					// mark this token as a search token
					end_type = tokens[token_position].type;
					tokens[token_position].search_match = true;
					token_position++;
				}
				if (token_position >= tokens.size() || tokens[token_position].start > match->match_end) {
					// insert the token that marks the end of the search
					highlightToken end_token;
					end_token.type = end_type;
					end_token.start = match->match_end;
					tokens.insert(tokens.begin() + token_position, end_token);
					token_position++;
				}
				break;
			}
		}
	}
	for (size_t i = 0; i < tokens.size(); i++) {
		size_t next = i + 1 < tokens.size() ? tokens[i + 1].start : len;
		if (next < start_pos) {
			// this token is not rendered at all
			continue;
		}

		auto &token = tokens[i];
		size_t start = token.start > start_pos ? token.start : start_pos;
		size_t end = next > end_pos ? end_pos : next;
		if (end <= start) {
			continue;
		}
		std::string text = std::string(buf + start, end - start);
		if (token.search_match) {
			ss << underline;
		}
		switch (token.type) {
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
			ss << keyword << text << reset;
			break;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
			ss << constant << text << reset;
			break;
		default:
			ss << text;
			if (token.search_match) {
				ss << reset;
			}
		}
	}
	return ss.str();
}
#endif

static void renderText(size_t &render_pos, char *&buf, size_t &len, size_t pos, size_t cols, size_t plen,
                       std::string &highlight_buffer, bool highlight, searchMatch *match = nullptr) {
	if (duckdb::Utf8Proc::IsValid(buf, len)) {
		// utf8 in prompt, handle rendering
		size_t remaining_render_width = cols - plen - 1;
		size_t start_pos = 0;
		size_t cpos = 0;
		size_t prev_pos = 0;
		size_t total_render_width = 0;
		while (cpos < len) {
			size_t char_render_width = duckdb::Utf8Proc::RenderWidth(buf, len, cpos);
			prev_pos = cpos;
			cpos = duckdb::Utf8Proc::NextGraphemeCluster(buf, len, cpos);
			total_render_width += cpos - prev_pos;
			if (total_render_width >= remaining_render_width) {
				// character does not fit anymore! we need to figure something out
				if (prev_pos >= pos) {
					// we passed the cursor: break
					cpos = prev_pos;
					break;
				} else {
					// we did not pass the cursor yet! remove characters from the start until it fits again
					while (total_render_width >= remaining_render_width) {
						size_t start_char_width = duckdb::Utf8Proc::RenderWidth(buf, len, start_pos);
						size_t new_start = duckdb::Utf8Proc::NextGraphemeCluster(buf, len, start_pos);
						total_render_width -= new_start - start_pos;
						start_pos = new_start;
						render_pos -= start_char_width;
					}
				}
			}
			if (prev_pos < pos) {
				render_pos += char_render_width;
			}
		}
#ifndef DISABLE_HIGHLIGHT
		if (highlight) {
			highlight_buffer = highlightText(buf, len, start_pos, cpos, match);
			buf = (char *)highlight_buffer.c_str();
			len = highlight_buffer.size();
		} else
#endif
		{
			buf = buf + start_pos;
			len = cpos - start_pos;
		}
	} else {
		// invalid UTF8: fallback
		while ((plen + pos) >= cols) {
			buf++;
			len--;
			pos--;
		}
		while (plen + len > cols) {
			len--;
		}
		render_pos = pos;
	}
}

/* Single line low level line refresh.
 *
 * Rewrite the currently edited line accordingly to the buffer content,
 * cursor position, and number of columns of the terminal. */
static void refreshSingleLine(struct linenoiseState *l) {
	char seq[64];
	size_t plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
	int fd = l->ofd;
	char *buf = l->buf;
	size_t len = l->len;
	struct abuf ab;
	size_t render_pos = 0;
	std::string highlight_buffer;

	renderText(render_pos, buf, len, l->pos, l->cols, plen, highlight_buffer, enableHighlighting);

	abInit(&ab);
	/* Cursor to left edge */
	snprintf(seq, 64, "\r");
	abAppend(&ab, seq, strlen(seq));
	/* Write the prompt and the current buffer content */
	abAppend(&ab, l->prompt, strlen(l->prompt));
	abAppend(&ab, buf, len);
	/* Show hits if any. */
	refreshShowHints(&ab, l, plen);
	/* Erase to right */
	snprintf(seq, 64, "\x1b[0K");
	abAppend(&ab, seq, strlen(seq));
	/* Move cursor to original position. */
	snprintf(seq, 64, "\r\x1b[%dC", (int)(render_pos + plen));
	abAppend(&ab, seq, strlen(seq));
	if (write(fd, ab.b, ab.len) == -1) {
	} /* Can't recover from write error. */
	abFree(&ab);
}

static void refreshSearch(struct linenoiseState *l) {
	std::string search_prompt;
	static const size_t SEARCH_PROMPT_RENDER_SIZE = 28;
	std::string no_matches_text = "(no matches)";
	bool no_matches = l->search_index >= l->search_matches.size();
	if (l->search_buf.empty()) {
		search_prompt = "search" + std::string(SEARCH_PROMPT_RENDER_SIZE - 8, ' ') + "> ";
		no_matches_text = "(type to search)";
	} else {
		std::string search_text;
		std::string matches_text;
		search_text += l->search_buf;
		if (!no_matches) {
			matches_text += std::to_string(l->search_index + 1);
			matches_text += "/" + std::to_string(l->search_matches.size());
		}
		size_t search_text_length = linenoiseComputeRenderWidth(search_text.c_str(), search_text.size());
		size_t matches_text_length = linenoiseComputeRenderWidth(matches_text.c_str(), matches_text.size());
		size_t total_text_length = search_text_length + matches_text_length;
		if (total_text_length < SEARCH_PROMPT_RENDER_SIZE - 2) {
			// search text is short: we can render the entire search text
			search_prompt = search_text;
			search_prompt += std::string(SEARCH_PROMPT_RENDER_SIZE - 2 - total_text_length, ' ');
			search_prompt += matches_text;
			search_prompt += "> ";
		} else {
			// search text length is too long to fit: truncate
			bool render_matches = matches_text_length < SEARCH_PROMPT_RENDER_SIZE - 8;
			char *search_buf = (char *)search_text.c_str();
			size_t search_len = search_text.size();
			size_t search_render_pos = 0;
			size_t max_render_size = SEARCH_PROMPT_RENDER_SIZE - 3;
			if (render_matches) {
				max_render_size -= matches_text_length;
			}
			std::string highlight_buffer;
			renderText(search_render_pos, search_buf, search_len, search_len, max_render_size, 0, highlight_buffer,
			           false);
			search_prompt = std::string(search_buf, search_len);
			for (size_t i = search_render_pos; i < max_render_size; i++) {
				search_prompt += " ";
			}
			if (render_matches) {
				search_prompt += matches_text;
			}
			search_prompt += "> ";
		}
	}

	char seq[64];
	size_t plen = linenoiseComputeRenderWidth(search_prompt.c_str(), search_prompt.size());
	int fd = l->ofd;
	char *buf;
	size_t len;
	size_t cols = l->cols;
	struct abuf ab;
	size_t render_pos = 0;
	std::string highlight_buffer;

	if (!no_matches) {
		// if there are matches render the current history item
		auto search_match = l->search_matches[l->search_index];
		auto history_index = search_match.history_index;
		auto cursor_position = search_match.match_end;
		buf = history[history_index];
		len = strlen(history[history_index]);
		renderText(render_pos, buf, len, cursor_position, cols, plen, highlight_buffer, enableHighlighting,
		           &search_match);
	}

	abInit(&ab);
	/* Cursor to left edge */
	snprintf(seq, 64, "\r");
	abAppend(&ab, seq, strlen(seq));
	/* Write the prompt and the current buffer content */
	abAppend(&ab, search_prompt.c_str(), search_prompt.size());
	if (no_matches) {
		abAppend(&ab, no_matches_text.c_str(), no_matches_text.size());
	} else {
		abAppend(&ab, buf, len);
	}
	/* Show hits if any. */
	refreshShowHints(&ab, l, plen);
	/* Erase to right */
	snprintf(seq, 64, "\x1b[0K");
	abAppend(&ab, seq, strlen(seq));
	/* Move cursor to original position. */
	snprintf(seq, 64, "\r\x1b[%dC", (int)(render_pos + plen));
	abAppend(&ab, seq, strlen(seq));
	if (write(fd, ab.b, ab.len) == -1) {
	} /* Can't recover from write error. */
	abFree(&ab);
}

/* Multi line low level line refresh.
 *
 * Rewrite the currently edited line accordingly to the buffer content,
 * cursor position, and number of columns of the terminal. */
static void refreshMultiLine(struct linenoiseState *l) {
	char seq[64];
	int plen = linenoiseComputeRenderWidth(l->prompt, strlen(l->prompt));
	int total_len = linenoiseComputeRenderWidth(l->buf, l->len);
	int cursor_old_pos = linenoiseComputeRenderWidth(l->buf, l->oldpos);
	int cursor_pos = linenoiseComputeRenderWidth(l->buf, l->pos);
	int rows = (plen + total_len + l->cols - 1) / l->cols;  /* rows used by current buf. */
	int rpos = (plen + cursor_old_pos + l->cols) / l->cols; /* cursor relative row. */
	int rpos2;                                              /* rpos after refresh. */
	int col;                                                /* colum position, zero-based. */
	int old_rows = l->maxrows;
	int fd = l->ofd, j;
	struct abuf ab;
	std::string highlight_buffer;
	auto buf = l->buf;
	auto len = l->len;

	/* Update maxrows if needed. */
	if (rows > (int)l->maxrows) {
		l->maxrows = rows;
	}

	if (duckdb::Utf8Proc::IsValid(l->buf, l->len)) {
#ifndef DISABLE_HIGHLIGHT
		if (enableHighlighting) {
			highlight_buffer = highlightText(buf, len, 0, len);
			buf = (char *)highlight_buffer.c_str();
			len = highlight_buffer.size();
		}
#endif
	}

	/* First step: clear all the lines used before. To do so start by
	 * going to the last row. */
	abInit(&ab);
	if (old_rows - rpos > 0) {
		lndebug("go down %d", old_rows - rpos);
		snprintf(seq, 64, "\x1b[%dB", old_rows - rpos);
		abAppend(&ab, seq, strlen(seq));
	}

	/* Now for every row clear it, go up. */
	for (j = 0; j < old_rows - 1; j++) {
		lndebug("clear+up", 0);
		snprintf(seq, 64, "\r\x1b[0K\x1b[1A");
		abAppend(&ab, seq, strlen(seq));
	}

	/* Clean the top line. */
	lndebug("clear", 0);
	snprintf(seq, 64, "\r\x1b[0K");
	abAppend(&ab, seq, strlen(seq));

	/* Write the prompt and the current buffer content */
	abAppend(&ab, l->prompt, strlen(l->prompt));
	abAppend(&ab, buf, len);

	/* Show hints if any. */
	refreshShowHints(&ab, l, plen);

	/* If we are at the very end of the screen with our prompt, we need to
	 * emit a newline and move the prompt to the first column. */
	if (l->pos && l->pos == len && (cursor_pos + plen) % l->cols == 0) {
		lndebug("<newline>", 0);
		abAppend(&ab, "\n", 1);
		snprintf(seq, 64, "\r");
		abAppend(&ab, seq, strlen(seq));
		rows++;
		if (rows > (int)l->maxrows)
			l->maxrows = rows;
	}

	/* Move cursor to right position. */
	rpos2 = (plen + cursor_pos + l->cols) / l->cols; /* current cursor relative row. */
	lndebug("rpos2 %d", rpos2);

	/* Go up till we reach the expected positon. */
	if (rows - rpos2 > 0) {
		lndebug("go-up %d", rows - rpos2);
		snprintf(seq, 64, "\x1b[%dA", rows - rpos2);
		abAppend(&ab, seq, strlen(seq));
	}

	/* Set column. */
	col = (plen + (int)cursor_pos) % (int)l->cols;
	lndebug("set col %d", 1 + col);
	if (col)
		snprintf(seq, 64, "\r\x1b[%dC", col);
	else
		snprintf(seq, 64, "\r");
	abAppend(&ab, seq, strlen(seq));

	lndebug("\n", 0);
	l->oldpos = l->pos;

	if (write(fd, ab.b, ab.len) == -1) {
	} /* Can't recover from write error. */
	abFree(&ab);
}

/* Calls the two low level functions refreshSingleLine() or
 * refreshMultiLine() according to the selected mode. */
static void refreshLine(struct linenoiseState *l) {
	if (mlmode)
		refreshMultiLine(l);
	else
		refreshSingleLine(l);
}

/* Insert the character 'c' at cursor current position.
 *
 * On error writing to the terminal -1 is returned, otherwise 0. */
int linenoiseEditInsert(struct linenoiseState *l, char c) {
	if (l->len < l->buflen) {
		if (l->len == l->pos) {
			l->buf[l->pos] = c;
			l->pos++;
			l->len++;
			l->buf[l->len] = '\0';
			if ((!mlmode && l->plen + l->len < l->cols && !hintsCallback)) {
				/* Avoid a full update of the line in the
				 * trivial case. */
				if (write(l->ofd, &c, 1) == -1)
					return -1;
			} else {
				refreshLine(l);
			}
		} else {
			memmove(l->buf + l->pos + 1, l->buf + l->pos, l->len - l->pos);
			l->buf[l->pos] = c;
			l->len++;
			l->pos++;
			l->buf[l->len] = '\0';
			refreshLine(l);
		}
	}
	refreshLine(l);
	return 0;
}

static size_t prev_char(struct linenoiseState *l) {
	return duckdb::Utf8Proc::PreviousGraphemeCluster(l->buf, l->len, l->pos);
}

static size_t next_char(struct linenoiseState *l) {
	return duckdb::Utf8Proc::NextGraphemeCluster(l->buf, l->len, l->pos);
}

/* Move cursor on the left. */
void linenoiseEditMoveLeft(struct linenoiseState *l) {
	if (l->pos > 0) {
		l->pos = prev_char(l);
		refreshLine(l);
	}
}

/* Move cursor on the right. */
void linenoiseEditMoveRight(struct linenoiseState *l) {
	if (l->pos != l->len) {
		l->pos = next_char(l);
		refreshLine(l);
	}
}

bool characterIsWordBoundary(char c) {
	if (c >= 'a' && c <= 'z') {
		return false;
	}
	if (c >= 'A' && c <= 'Z') {
		return false;
	}
	if (c >= '0' && c <= '9') {
		return false;
	}
	return true;
}

/* Move cursor to the next left word. */
void linenoiseEditMoveWordLeft(struct linenoiseState *l) {
	if (l->pos == 0) {
		return;
	}
	do {
		l->pos = prev_char(l);
	} while (l->pos > 0 && !characterIsWordBoundary(l->buf[l->pos]));
	refreshLine(l);
}

/* Move cursor on the right. */
void linenoiseEditMoveWordRight(struct linenoiseState *l) {
	if (l->pos == l->len) {
		return;
	}
	do {
		l->pos = next_char(l);
	} while (l->pos != l->len && !characterIsWordBoundary(l->buf[l->pos]));
	refreshLine(l);
}

/* Move cursor to the start of the line. */
void linenoiseEditMoveHome(struct linenoiseState *l) {
	if (l->pos != 0) {
		l->pos = 0;
		refreshLine(l);
	}
}

/* Move cursor to the end of the line. */
void linenoiseEditMoveEnd(struct linenoiseState *l) {
	if (l->pos != l->len) {
		l->pos = l->len;
		refreshLine(l);
	}
}

/* Substitute the currently edited line with the next or previous history
 * entry as specified by 'dir'. */
#define LINENOISE_HISTORY_NEXT 0
#define LINENOISE_HISTORY_PREV 1
void linenoiseEditHistoryNext(struct linenoiseState *l, int dir) {
	if (history_len > 1) {
		/* Update the current history entry before to
		 * overwrite it with the next one. */
		free(history[history_len - 1 - l->history_index]);
		history[history_len - 1 - l->history_index] = strdup(l->buf);
		/* Show the new entry */
		l->history_index += (dir == LINENOISE_HISTORY_PREV) ? 1 : -1;
		if (l->history_index < 0) {
			l->history_index = 0;
			return;
		} else if (l->history_index >= history_len) {
			l->history_index = history_len - 1;
			return;
		}
		strncpy(l->buf, history[history_len - 1 - l->history_index], l->buflen);
		l->buf[l->buflen - 1] = '\0';
		l->len = l->pos = strlen(l->buf);
		refreshLine(l);
	}
}

/* Delete the character at the right of the cursor without altering the cursor
 * position. Basically this is what happens with the "Delete" keyboard key. */
void linenoiseEditDelete(struct linenoiseState *l) {
	if (l->len > 0 && l->pos < l->len) {
		size_t new_pos = next_char(l);
		size_t char_sz = new_pos - l->pos;
		memmove(l->buf + l->pos, l->buf + new_pos, l->len - new_pos);
		l->len -= char_sz;
		l->buf[l->len] = '\0';
		refreshLine(l);
	}
}

/* Backspace implementation. */
void linenoiseEditBackspace(struct linenoiseState *l) {
	if (l->pos > 0 && l->len > 0) {
		size_t new_pos = prev_char(l);
		size_t char_sz = l->pos - new_pos;
		memmove(l->buf + new_pos, l->buf + l->pos, l->len - l->pos);
		l->len -= char_sz;
		l->pos = new_pos;
		l->buf[l->len] = '\0';
		refreshLine(l);
	}
}

/* Delete the previous word, maintaining the cursor at the start of the
 * current word. */
void linenoiseEditDeletePrevWord(struct linenoiseState *l) {
	size_t old_pos = l->pos;
	size_t diff;

	while (l->pos > 0 && l->buf[l->pos - 1] == ' ')
		l->pos--;
	while (l->pos > 0 && l->buf[l->pos - 1] != ' ')
		l->pos--;
	diff = old_pos - l->pos;
	memmove(l->buf + l->pos, l->buf + old_pos, l->len - old_pos + 1);
	l->len -= diff;
	refreshLine(l);
}

// returns true if there is more data available to read in a particular stream
static int hasMoreData(int fd) {
	fd_set rfds;
	FD_ZERO(&rfds);
	FD_SET(fd, &rfds);

	// no timeout: return immediately
	struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 0;
	return select(1, &rfds, NULL, NULL, &tv);
}

static void cancelSearch(linenoiseState *l) {
	l->search = false;
	l->search_buf = std::string();
	l->search_matches.clear();
	l->search_index = 0;
	refreshLine(l);
}

static char acceptSearch(linenoiseState *l, char nextCommand) {
	if (l->search_index < l->search_matches.size()) {
		// if there is a match - copy it into the buffer
		auto match = l->search_matches[l->search_index];
		auto history_entry = history[match.history_index];
		auto history_len = strlen(history_entry);
		memcpy(l->buf, history_entry, history_len);
		l->buf[history_len] = '\0';
		l->pos = match.match_end;
		l->len = history_len;
	}
	cancelSearch(l);
	return nextCommand;
}

static void performSearch(linenoiseState *l) {
	// we try to maintain the current match while searching
	size_t current_match = history_len;
	if (l->search_index < l->search_matches.size()) {
		current_match = l->search_matches[l->search_index].history_index;
	}
	l->search_matches.clear();
	l->search_index = 0;
	if (l->search_buf.empty()) {
		return;
	}
	std::unordered_set<std::string> matches;
	auto lsearch = duckdb::StringUtil::Lower(l->search_buf);
	for (size_t i = history_len; i > 0; i--) {
		size_t history_index = i - 1;
		auto lhistory = duckdb::StringUtil::Lower(history[history_index]);
		if (matches.find(lhistory) != matches.end()) {
			continue;
		}
		matches.insert(lhistory);
		auto entry = lhistory.find(lsearch);
		if (entry != duckdb::string::npos) {
			if (history_index == current_match) {
				l->search_index = l->search_matches.size();
			}
			searchMatch match;
			match.history_index = history_index;
			match.match_start = entry;
			match.match_end = entry + lsearch.size();
			l->search_matches.push_back(match);
		}
	}
}

static void searchPrev(linenoiseState *l) {
	if (l->search_index > 0) {
		l->search_index--;
	} else if (l->search_matches.size() > 0) {
		l->search_index = l->search_matches.size() - 1;
	}
}

static void searchNext(linenoiseState *l) {
	l->search_index += 1;
	if (l->search_index >= l->search_matches.size()) {
		l->search_index = 0;
	}
}

static char linenoiseSearch(linenoiseState *l, char c) {
	char seq[64];

	switch (c) {
	case 10:
	case ENTER: /* enter */
		// accept search and run
		return acceptSearch(l, ENTER);
	case CTRL_R:
		// move to the next match index
		searchNext(l);
		break;
	case ESC: /* escape sequence */
		/* Read the next two bytes representing the escape sequence.
		 * Use two calls to handle slow terminals returning the two
		 * chars at different times. */
		// note: in search mode we ignore almost all special commands
		if (read(l->ifd, seq, 1) == -1)
			break;
		if (seq[0] == ESC) {
			// double escape accepts search without any additional command
			return acceptSearch(l, 0);
		}
		if (seq[0] == 'b' || seq[0] == 'f') {
			break;
		}
		if (read(l->ifd, seq + 1, 1) == -1)
			break;

		/* ESC [ sequences. */
		if (seq[0] == '[') {
			if (seq[1] >= '0' && seq[1] <= '9') {
				/* Extended escape, read additional byte. */
				if (read(l->ifd, seq + 2, 1) == -1)
					break;
				if (seq[2] == '~') {
					switch (seq[1]) {
					case '1':
						return acceptSearch(l, CTRL_A);
					case '4':
					case '8':
						return acceptSearch(l, CTRL_E);
					default:
						break;
					}
				} else if (seq[2] == ';') {
					// read 2 extra bytes
					if (read(l->ifd, seq + 3, 2) == -1)
						break;
				}
			} else {
				switch (seq[1]) {
				case 'A': /* Up */
					searchPrev(l);
					break;
				case 'B': /* Down */
					searchNext(l);
					break;
				case 'D': /* Left */
					return acceptSearch(l, CTRL_B);
				case 'C': /* Right */
					return acceptSearch(l, CTRL_F);
				case 'H': /* Home */
					return acceptSearch(l, CTRL_A);
				case 'F': /* End*/
					return acceptSearch(l, CTRL_E);
				default:
					break;
				}
			}
		}
		/* ESC O sequences. */
		else if (seq[0] == 'O') {
			switch (seq[1]) {
			case 'H': /* Home */
				return acceptSearch(l, CTRL_A);
			case 'F': /* End*/
				return acceptSearch(l, CTRL_E);
			default:
				break;
			}
		}
		break;
	case CTRL_A: // accept search, move to start of line
		return acceptSearch(l, CTRL_A);
	case '\t':
	case CTRL_E: // accept search - move to end of line
		return acceptSearch(l, CTRL_E);
	case CTRL_B: // accept search - move cursor left
		return acceptSearch(l, CTRL_B);
	case CTRL_F: // accept search - move cursor right
		return acceptSearch(l, CTRL_F);
	case CTRL_T: // accept search: swap character
		return acceptSearch(l, CTRL_T);
	case CTRL_U: // accept search, clear buffer
		return acceptSearch(l, CTRL_U);
	case CTRL_K: // accept search, clear after cursor
		return acceptSearch(l, CTRL_K);
	case CTRL_D: // accept saerch, delete a character
		return acceptSearch(l, CTRL_D);
	case CTRL_L:
		linenoiseClearScreen();
		break;
	case CTRL_P:
		searchPrev(l);
		break;
	case CTRL_N:
		searchNext(l);
		break;
	case CTRL_C:
	case CTRL_G:
		// abort search
		cancelSearch(l);
		return 0;
	case BACKSPACE: /* backspace */
	case 8:         /* ctrl-h */
	case CTRL_W:    /* ctrl-w */
		// remove trailing UTF-8 bytes (if any)
		while (!l->search_buf.empty() && ((l->search_buf.back() & 0xc0) == 0x80)) {
			l->search_buf.pop_back();
		}
		// finally remove the first UTF-8 byte
		if (!l->search_buf.empty()) {
			l->search_buf.pop_back();
		}
		performSearch(l);
		break;
	default:
		// add input to search buffer
		l->search_buf += c;
		// perform the search
		performSearch(l);
		break;
	}
	refreshSearch(l);
	return 0;
}

/* This function is the core of the line editing capability of linenoise.
 * It expects 'fd' to be already in "raw mode" so that every key pressed
 * will be returned ASAP to read().
 *
 * The resulting string is put into 'buf' when the user type enter, or
 * when ctrl+d is typed.
 *
 * The function returns the length of the current buffer. */
static int linenoiseEdit(int stdin_fd, int stdout_fd, char *buf, size_t buflen, const char *prompt) {
	struct linenoiseState l;

	/* Populate the linenoise state that we pass to functions implementing
	 * specific editing functionalities. */
	l.ifd = stdin_fd;
	l.ofd = stdout_fd;
	l.buf = buf;
	l.buflen = buflen;
	l.prompt = prompt;
	l.plen = strlen(prompt);
	l.oldpos = l.pos = 0;
	l.len = 0;
	l.cols = getColumns(stdin_fd, stdout_fd);
	l.maxrows = 0;
	l.history_index = 0;
	l.search = false;

	/* Buffer starts empty. */
	l.buf[0] = '\0';
	l.buflen--; /* Make sure there is always space for the nulterm */

	/* The latest history entry is always our current buffer, that
	 * initially is just an empty string. */
	linenoiseHistoryAdd("");

	if (write(l.ofd, prompt, l.plen) == -1)
		return -1;
	while (1) {
		char c;
		int nread;
		char seq[5];

		nread = read(l.ifd, &c, 1);
		if (nread <= 0)
			return l.len;

		if (l.search) {
			char ret = linenoiseSearch(&l, c);
			if (l.search || ret == '\0') {
				// still searching - continue searching
				continue;
			}
			// run subsequent command
			c = ret;
		}

		/* Only autocomplete when the callback is set. It returns < 0 when
		 * there was an error reading from fd. Otherwise it will return the
		 * character that should be handled next. */
		if (c == 9 && completionCallback != NULL) {
			if (hasMoreData(l.ifd)) {
				// if there is more data, this tab character was added as part of copy-pasting data
				continue;
			}
			c = completeLine(&l);
			/* Return on errors */
			if (c < 0)
				return l.len;
			/* Read next character when 0 */
			if (c == 0)
				continue;
		}

		lndebug("%d\n", (int)c);
		switch (c) {
		case 10:
		case ENTER: /* enter */
			history_len--;
			free(history[history_len]);
			if (mlmode) {
				linenoiseEditMoveEnd(&l);
			}
			if (hintsCallback) {
				/* Force a refresh without hints to leave the previous
				 * line as the user typed it after a newline. */
				linenoiseHintsCallback *hc = hintsCallback;
				hintsCallback = NULL;
				refreshLine(&l);
				hintsCallback = hc;
			}
			return (int)l.len;
		case CTRL_G:
		case CTRL_C: /* ctrl-c */ {
			l.buf[0] = '\3';
			// we keep track of whether or not the line was empty by writing \3 to the second position of the line
			// this is because at a higher level we might want to know if we pressed ctrl c to clear the line
			// or to exit the process
			if (l.len > 0) {
				l.buf[1] = '\3';
				l.buf[2] = '\0';
				l.pos = 2;
				l.len = 2;
			} else {
				l.buf[1] = '\0';
				l.pos = 1;
				l.len = 1;
			}
			return (int)l.len;
		}
		case BACKSPACE: /* backspace */
		case 8:         /* ctrl-h */
			linenoiseEditBackspace(&l);
			break;
		case CTRL_D: /* ctrl-d, remove char at right of cursor, or if the
		                line is empty, act as end-of-file. */
			if (l.len > 0) {
				linenoiseEditDelete(&l);
			} else {
				history_len--;
				free(history[history_len]);
				return -1;
			}
			break;
		case CTRL_T: /* ctrl-t, swaps current character with previous. */
			if (l.pos > 0 && l.pos < l.len) {
				char temp_buffer[128];
				int prev_pos = prev_char(&l);
				int next_pos = next_char(&l);
				int prev_char_size = l.pos - prev_pos;
				int cur_char_size = next_pos - l.pos;
				memcpy(temp_buffer, l.buf + prev_pos, prev_char_size);
				memmove(l.buf + prev_pos, l.buf + l.pos, cur_char_size);
				memcpy(l.buf + prev_pos + cur_char_size, temp_buffer, prev_char_size);
				l.pos = next_pos;
				refreshLine(&l);
			}
			break;
		case CTRL_B: /* ctrl-b */
			linenoiseEditMoveLeft(&l);
			break;
		case CTRL_F: /* ctrl-f */
			linenoiseEditMoveRight(&l);
			break;
		case CTRL_P: /* ctrl-p */
			linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_PREV);
			break;
		case CTRL_N: /* ctrl-n */
			linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_NEXT);
			break;
		case CTRL_R: /* ctrl-r */ {
			// initiate reverse search
			l.search = true;
			l.search_buf = std::string();
			l.search_matches.clear();
			l.search_index = 0;
			refreshSearch(&l);
			break;
		}
		case ESC: /* escape sequence */
			/* Read the next two bytes representing the escape sequence.
			 * Use two calls to handle slow terminals returning the two
			 * chars at different times. */
			if (read(l.ifd, seq, 1) == -1)
				break;
			if (seq[0] == 'b') {
				linenoiseEditMoveWordLeft(&l);
				break;
			} else if (seq[0] == 'f') {
				linenoiseEditMoveWordRight(&l);
				break;
			}
			// lndebug("seq0: %d\n", seq[0]);
			if (read(l.ifd, seq + 1, 1) == -1)
				break;
			// lndebug("seq1: %d\n", seq[1]);

			/* ESC [ sequences. */
			if (seq[0] == '[') {
				if (seq[1] >= '0' && seq[1] <= '9') {
					/* Extended escape, read additional byte. */
					if (read(l.ifd, seq + 2, 1) == -1)
						break;
					if (seq[2] == '~') {
						switch (seq[1]) {
						case '1':
							linenoiseEditMoveHome(&l);
							break;
						case '3': /* Delete key. */
							linenoiseEditDelete(&l);
							break;
						case '4':
							linenoiseEditMoveEnd(&l);
							break;
						case '8':
							linenoiseEditMoveEnd(&l);
							break;
						default:
							lndebug("unrecognized escape sequence (~) %d", seq[1]);
							break;
						}
					} else if (seq[2] == ';') {
						// read 2 extra bytes
						if (read(l.ifd, seq + 3, 2) == -1)
							break;
						if (memcmp(seq, "[1;5C", 5) == 0) {
							// [1;5C: move word right
							linenoiseEditMoveWordRight(&l);
						} else if (memcmp(seq, "[1;5D", 5) == 0) {
							// [1;5D: move word left
							linenoiseEditMoveWordLeft(&l);
						} else {
							lndebug("unrecognized escape sequence (;) %d", seq[1]);
						}
					} else if (seq[1] == '5' && seq[2] == 'C') {
						linenoiseEditMoveWordRight(&l);
					} else if (seq[1] == '5' && seq[2] == 'D') {
						linenoiseEditMoveWordLeft(&l);
					}
				} else {
					switch (seq[1]) {
					case 'A': /* Up */
						linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_PREV);
						break;
					case 'B': /* Down */
						linenoiseEditHistoryNext(&l, LINENOISE_HISTORY_NEXT);
						break;
					case 'C': /* Right */
						linenoiseEditMoveRight(&l);
						break;
					case 'D': /* Left */
						linenoiseEditMoveLeft(&l);
						break;
					case 'H': /* Home */
						linenoiseEditMoveHome(&l);
						break;
					case 'F': /* End*/
						linenoiseEditMoveEnd(&l);
						break;
					default:
						lndebug("unrecognized escape sequence (seq[1]) %d", seq[1]);
						break;
					}
				}
			}
			/* ESC O sequences. */
			else if (seq[0] == 'O') {
				switch (seq[1]) {
				case 'H': /* Home */
					linenoiseEditMoveHome(&l);
					break;
				case 'F': /* End*/
					linenoiseEditMoveEnd(&l);
					break;
				case 'c':
					linenoiseEditMoveWordRight(&l);
					break;
				case 'd':
					linenoiseEditMoveWordLeft(&l);
					break;
				default:
					lndebug("unrecognized escape sequence (O) %d", seq[1]);
					break;
				}
			}
			break;
		case CTRL_U: /* Ctrl+u, delete the whole line. */
			buf[0] = '\0';
			l.pos = l.len = 0;
			refreshLine(&l);
			break;
		case CTRL_K: /* Ctrl+k, delete from current to end of line. */
			buf[l.pos] = '\0';
			l.len = l.pos;
			refreshLine(&l);
			break;
		case CTRL_A: /* Ctrl+a, go to the start of the line */
			linenoiseEditMoveHome(&l);
			break;
		case CTRL_E: /* ctrl+e, go to the end of the line */
			linenoiseEditMoveEnd(&l);
			break;
		case CTRL_L: /* ctrl+l, clear screen */
			linenoiseClearScreen();
			refreshLine(&l);
			break;
		case CTRL_W: /* ctrl+w, delete previous word */
			linenoiseEditDeletePrevWord(&l);
			break;
		default: {
			if (linenoiseEditInsert(&l, c)) {
				return -1;
			}
			break;
		}
		}
	}
	return l.len;
}

/* This special mode is used by linenoise in order to print scan codes
 * on screen for debugging / development purposes. It is implemented
 * by the linenoise_example program using the --keycodes option. */
void linenoisePrintKeyCodes(void) {
	char quit[4];

	printf("Linenoise key codes debugging mode.\n"
	       "Press keys to see scan codes. Type 'quit' at any time to exit.\n");
	if (enableRawMode(STDIN_FILENO) == -1)
		return;
	memset(quit, ' ', 4);
	while (1) {
		char c;
		int nread;

		nread = read(STDIN_FILENO, &c, 1);
		if (nread <= 0)
			continue;
		memmove(quit, quit + 1, sizeof(quit) - 1); /* shift string to left. */
		quit[sizeof(quit) - 1] = c;                /* Insert current char on the right. */
		if (memcmp(quit, "quit", sizeof(quit)) == 0)
			break;

		printf("'%c' %02x (%d) (type quit to exit)\n", isprint(c) ? c : '?', (int)c, (int)c);
		printf("\r"); /* Go left edge manually, we are in raw mode. */
		fflush(stdout);
	}
	disableRawMode(STDIN_FILENO);
}

/* This function calls the line editing function linenoiseEdit() using
 * the STDIN file descriptor set in raw mode. */
static int linenoiseRaw(char *buf, size_t buflen, const char *prompt) {
	int count;

	if (buflen == 0) {
		errno = EINVAL;
		return -1;
	}

	if (enableRawMode(STDIN_FILENO) == -1)
		return -1;
	count = linenoiseEdit(STDIN_FILENO, STDOUT_FILENO, buf, buflen, prompt);
	disableRawMode(STDIN_FILENO);
	printf("\n");
	return count;
}

/* This function is called when linenoise() is called with the standard
 * input file descriptor not attached to a TTY. So for example when the
 * program using linenoise is called in pipe or with a file redirected
 * to its standard input. In this case, we want to be able to return the
 * line regardless of its length (by default we are limited to 4k). */
static char *linenoiseNoTTY(void) {
	char *line = NULL;
	size_t len = 0, maxlen = 0;

	while (1) {
		if (len == maxlen) {
			if (maxlen == 0)
				maxlen = 16;
			maxlen *= 2;
			char *oldval = line;
			line = (char *)realloc(line, maxlen);
			if (line == NULL) {
				if (oldval)
					free(oldval);
				return NULL;
			}
		}
		int c = fgetc(stdin);
		if (c == EOF || c == '\n') {
			if (c == EOF && len == 0) {
				free(line);
				return NULL;
			} else {
				line[len] = '\0';
				return line;
			}
		} else {
			line[len] = c;
			len++;
		}
	}
}

/* The high level function that is the main API of the linenoise library.
 * This function checks if the terminal has basic capabilities, just checking
 * for a blacklist of stupid terminals, and later either calls the line
 * editing function or uses dummy fgets() so that you will be able to type
 * something even in the most desperate of the conditions. */
char *linenoise(const char *prompt) {
	char buf[LINENOISE_MAX_LINE];
	int count;

	if (!isatty(STDIN_FILENO)) {
		/* Not a tty: read from file / pipe. In this mode we don't want any
		 * limit to the line size, so we call a function to handle that. */
		return linenoiseNoTTY();
	} else if (isUnsupportedTerm()) {
		size_t len;

		printf("%s", prompt);
		fflush(stdout);
		if (fgets(buf, LINENOISE_MAX_LINE, stdin) == NULL)
			return NULL;
		len = strlen(buf);
		while (len && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
			len--;
			buf[len] = '\0';
		}
		return strdup(buf);
	} else {
		count = linenoiseRaw(buf, LINENOISE_MAX_LINE, prompt);
		if (count == -1)
			return NULL;
		return strdup(buf);
	}
}

/* This is just a wrapper the user may want to call in order to make sure
 * the linenoise returned buffer is freed with the same allocator it was
 * created with. Useful when the main program is using an alternative
 * allocator. */
void linenoiseFree(void *ptr) {
	free(ptr);
}

/* ================================ History ================================= */

/* Free the history, but does not reset it. Only used when we have to
 * exit() to avoid memory leaks are reported by valgrind & co. */
static void freeHistory(void) {
	if (history) {
		int j;

		for (j = 0; j < history_len; j++)
			free(history[j]);
		free(history);
	}
}

/* At exit we'll try to fix the terminal to the initial conditions. */
static void linenoiseAtExit(void) {
	disableRawMode(STDIN_FILENO);
	freeHistory();
}

/* This is the API call to add a new entry in the linenoise history.
 * It uses a fixed array of char pointers that are shifted (memmoved)
 * when the history max length is reached in order to remove the older
 * entry and make room for the new one, so it is not exactly suitable for huge
 * histories, but will work well for a few hundred of entries.
 *
 * Using a circular buffer is smarter, but a bit more complex to handle. */
int linenoiseHistoryAdd(const char *line) {
	char *linecopy;

	if (history_max_len == 0)
		return 0;

	/* Initialization on first call. */
	if (history == NULL) {
		history = (char **)malloc(sizeof(char *) * history_max_len);
		if (history == NULL)
			return 0;
		memset(history, 0, (sizeof(char *) * history_max_len));
	}

	/* Don't add duplicated lines. */
	if (history_len && !strcmp(history[history_len - 1], line))
		return 0;

	/* Add an heap allocated copy of the line in the history.
	 * If we reached the max length, remove the older line. */
	linecopy = strdup(line);
	if (!linecopy)
		return 0;
	// replace all newlines with spaces
	for (auto ptr = linecopy; *ptr; ptr++) {
		if (*ptr == '\n' || *ptr == '\r') {
			*ptr = ' ';
		}
	}
	if (history_len == history_max_len) {
		free(history[0]);
		memmove(history, history + 1, sizeof(char *) * (history_max_len - 1));
		history_len--;
	}
	history[history_len] = linecopy;
	history_len++;
	if (history_file && strlen(line) > 0) {
		// if there is a history file that we loaded from
		// append to the history
		// this way we can recover history in case of a crash
		FILE *fp;

		fp = fopen(history_file, "a");
		if (fp == NULL) {
			return 1;
		}
		fprintf(fp, "%s\n", line);
		fclose(fp);
	}
	return 1;
}

/* Set the maximum length for the history. This function can be called even
 * if there is already some history, the function will make sure to retain
 * just the latest 'len' elements if the new history length value is smaller
 * than the amount of items already inside the history. */
int linenoiseHistorySetMaxLen(int len) {
	char **new_entry;

	if (len < 1)
		return 0;
	if (history) {
		int tocopy = history_len;

		new_entry = (char **)malloc(sizeof(char *) * len);
		if (new_entry == NULL)
			return 0;

		/* If we can't copy everything, free the elements we'll not use. */
		if (len < tocopy) {
			int j;

			for (j = 0; j < tocopy - len; j++)
				free(history[j]);
			tocopy = len;
		}
		memset(new_entry, 0, sizeof(char *) * len);
		memcpy(new_entry, history + (history_len - tocopy), sizeof(char *) * tocopy);
		free(history);
		history = new_entry;
	}
	history_max_len = len;
	if (history_len > history_max_len)
		history_len = history_max_len;
	return 1;
}

/* Save the history in the specified file. On success 0 is returned
 * otherwise -1 is returned. */
int linenoiseHistorySave(const char *filename) {
	mode_t old_umask = umask(S_IXUSR | S_IRWXG | S_IRWXO);
	FILE *fp;
	int j;

	fp = fopen(filename, "w");
	umask(old_umask);
	if (fp == NULL)
		return -1;
	chmod(filename, S_IRUSR | S_IWUSR);
	for (j = 0; j < history_len; j++)
		fprintf(fp, "%s\n", history[j]);
	fclose(fp);
	return 0;
}

/* Load the history from the specified file. If the file does not exist
 * zero is returned and no operation is performed.
 *
 * If the file exists and the operation succeeded 0 is returned, otherwise
 * on error -1 is returned. */
int linenoiseHistoryLoad(const char *filename) {
	FILE *fp = fopen(filename, "r");
	char buf[LINENOISE_MAX_LINE];

	if (fp == NULL) {
		return -1;
	}

	while (fgets(buf, LINENOISE_MAX_LINE, fp) != NULL) {
		char *p;

		p = strchr(buf, '\r');
		if (!p) {
			p = strchr(buf, '\n');
		}
		if (p) {
			*p = '\0';
		}
		linenoiseHistoryAdd(buf);
	}
	fclose(fp);

	history_file = strdup(filename);
	return 0;
}
