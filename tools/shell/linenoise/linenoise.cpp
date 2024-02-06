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

#include <sys/stat.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include "linenoise.h"
#include "linenoise.hpp"
#include "history.hpp"
#include "highlighting.hpp"
#include "terminal.hpp"
#include "utf8proc_wrapper.hpp"
#include <unordered_set>
#include <vector>
#include "duckdb_shell_wrapper.h"
#include <string>
#include "sqlite3.h"
#include "duckdb/common/string_util.hpp"
#ifdef __MVS__
#include <strings.h>
#include <sys/time.h>
#endif

namespace duckdb {

static linenoiseCompletionCallback *completionCallback = NULL;
static linenoiseHintsCallback *hintsCallback = NULL;
static linenoiseFreeHintsCallback *freeHintsCallback = NULL;

int linenoiseHistoryAdd(const char *line);

/* ============================== Completion ================================ */

/* Free a list of completion option populated by linenoiseAddCompletion(). */
static void freeCompletions(linenoiseCompletions *lc) {
	size_t i;
	for (i = 0; i < lc->len; i++) {
		free(lc->cvec[i]);
	}
	if (lc->cvec != nullptr) {
		free(lc->cvec);
	}
}

/* Register a callback function to be called for tab-completion. */
void Linenoise::SetCompletionCallback(linenoiseCompletionCallback *fn) {
	completionCallback = fn;
}

/* Register a hits function to be called to show hits to the user at the
 * right of the prompt. */
void Linenoise::SetHintsCallback(linenoiseHintsCallback *fn) {
	hintsCallback = fn;
}

/* Register a function to free the hints returned by the hints callback
 * registered with linenoiseSetHintsCallback(). */
void Linenoise::SetFreeHintsCallback(linenoiseFreeHintsCallback *fn) {
	freeHintsCallback = fn;
}

linenoiseHintsCallback *Linenoise::HintsCallback() {
	return hintsCallback;
}

linenoiseFreeHintsCallback *Linenoise::FreeHintsCallback() {
	return freeHintsCallback;
}

/* This is an helper function for linenoiseEdit() and is called when the
 * user types the <tab> key in order to complete the string currently in the
 * input.
 *
 * The state of the editing is encapsulated into the pointed linenoiseState
 * structure as described in the structure definition. */
int Linenoise::CompleteLine() {
	linenoiseCompletions lc = {0, NULL};
	int nread, nwritten;
	char c = 0;

	completionCallback(buf, &lc);
	if (lc.len == 0) {
		Terminal::Beep();
	} else {
		size_t stop = 0, i = 0;

		while (!stop) {
			/* Show completion or original buffer */
			if (i < lc.len) {
				Linenoise saved = *this;

				len = pos = strlen(lc.cvec[i]);
				buf = lc.cvec[i];
				RefreshLine();
				len = saved.len;
				pos = saved.pos;
				buf = saved.buf;
			} else {
				RefreshLine();
			}

			nread = read(ifd, &c, 1);
			if (nread <= 0) {
				freeCompletions(&lc);
				return -1;
			}

			switch (c) {
			case 9: /* tab */
				i = (i + 1) % (lc.len + 1);
				if (i == lc.len) {
					Terminal::Beep();
				}
				break;
			case 27: /* escape */
				/* Re-show original buffer */
				if (i < lc.len) {
					RefreshLine();
				}
				stop = 1;
				break;
			default:
				/* Update buffer and return */
				if (i < lc.len) {
					nwritten = snprintf(buf, buflen, "%s", lc.cvec[i]);
					len = pos = nwritten;
				}
				stop = 1;
				break;
			}
		}
	}

	freeCompletions(&lc);
	return c; /* Return last read character */
}

size_t Linenoise::ComputeRenderWidth(const char *buf, size_t len) {
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

int Linenoise::GetPromptWidth() const {
	return int(ComputeRenderWidth(prompt, strlen(prompt)));
}

int Linenoise::GetRenderPosition(const char *buf, size_t len, int max_width, int *n) {
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

int Linenoise::ParseOption(const char **azArg, int nArg, const char **out_error) {
	if (strcmp(azArg[0], "highlight") == 0) {
		if (nArg == 2) {
			if (strcmp(azArg[1], "off") == 0 || strcmp(azArg[1], "0") == 0) {
				Highlighting::Disable();
				return 1;
			} else if (strcmp(azArg[1], "on") == 0 || strcmp(azArg[1], "1") == 0) {
				Highlighting::Enable();
				return 1;
			}
		}
		*out_error = "Expected usage: .highlight [off|on]";
		return 1;
	} else if (strcmp(azArg[0], "keyword") == 0) {
		if (nArg == 2) {
			const char *option = Highlighting::GetColorOption(azArg[1]);
			if (option) {
				Highlighting::SetKeyword(option);
				return 1;
			}
		}
		*out_error = "Expected usage: .keyword "
		             "[red|green|yellow|blue|magenta|cyan|white|brightblack|brightred|brightgreen|brightyellow|"
		             "brightblue|brightmagenta|brightcyan|brightwhite]";
		return 1;
	} else if (strcmp(azArg[0], "constant") == 0) {
		if (nArg == 2) {
			const char *option = Highlighting::GetColorOption(azArg[1]);
			if (option) {
				Highlighting::SetConstant(option);
				return 1;
			}
		}
		*out_error = "Expected usage: .constant "
		             "[red|green|yellow|blue|magenta|cyan|white|brightblack|brightred|brightgreen|brightyellow|"
		             "brightblue|brightmagenta|brightcyan|brightwhite]";
		return 1;
	} else if (strcmp(azArg[0], "keywordcode") == 0) {
		if (nArg == 2) {
			Highlighting::SetKeyword(azArg[1]);
			return 1;
		}
		*out_error = "Expected usage: .keywordcode [terminal_code]";
		return 1;
	} else if (strcmp(azArg[0], "constantcode") == 0) {
		if (nArg == 2) {
			Highlighting::SetConstant(azArg[1]);
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
	return 0;
}

bool Linenoise::IsNewline(char c) {
	return c == '\r' || c == '\n';
}

void Linenoise::NextPosition(const char *buf, size_t len, size_t &cpos, int &rows, int &cols, int plen) const {
	if (IsNewline(buf[cpos])) {
		// explicit newline! move to next line and insert a prompt
		rows++;
		cols = plen;
		cpos++;
		if (buf[cpos - 1] == '\r' && cpos < len && buf[cpos] == '\n') {
			cpos++;
		}
		return;
	}
	int sz;
	int char_render_width;
	if (duckdb::Utf8Proc::UTF8ToCodepoint(buf + cpos, sz) < 0) {
		char_render_width = 1;
		cpos++;
	} else {
		char_render_width = (int)duckdb::Utf8Proc::RenderWidth(buf, len, cpos);
		cpos = duckdb::Utf8Proc::NextGraphemeCluster(buf, len, cpos);
	}
	if (cols + char_render_width > ws.ws_col) {
		// exceeded l->cols, move to next row
		rows++;
		cols = char_render_width;
	}
	cols += char_render_width;
}

void Linenoise::PositionToColAndRow(size_t target_pos, int &out_row, int &out_col, int &rows, int &cols) const {
	int plen = GetPromptWidth();
	out_row = -1;
	out_col = 0;
	rows = 1;
	cols = plen;
	size_t cpos = 0;
	while (cpos < len) {
		if (cols >= ws.ws_col && !IsNewline(buf[cpos])) {
			// exceeded width - move to next line
			rows++;
			cols = 0;
		}
		if (out_row < 0 && cpos >= target_pos) {
			out_row = rows;
			out_col = cols;
		}
		NextPosition(buf, len, cpos, rows, cols, plen);
	}
	if (target_pos == len) {
		out_row = rows;
		out_col = cols;
	}
}

size_t Linenoise::ColAndRowToPosition(int target_row, int target_col) const {
	int plen = GetPromptWidth();
	int rows = 1;
	int cols = plen;
	size_t last_cpos = 0;
	size_t cpos = 0;
	while (cpos < len) {
		if (cols >= ws.ws_col) {
			// exceeded width - move to next line
			rows++;
			cols = 0;
		}
		if (rows > target_row) {
			// we have skipped our target row - that means "target_col" was out of range for this row
			// return the last position within the target row
			return last_cpos;
		}
		if (rows == target_row) {
			last_cpos = cpos;
		}
		if (rows == target_row && cols == target_col) {
			return cpos;
		}
		NextPosition(buf, len, cpos, rows, cols, plen);
	}
	return cpos;
}

/* Insert the character 'c' at cursor current position.
 *
 * On error writing to the terminal -1 is returned, otherwise 0. */
void Linenoise::InsertCharacter(char c) {
	if (len < buflen) {
		if (len == pos) {
			buf[pos] = c;
			pos++;
			len++;
			buf[len] = '\0';
		} else {
			memmove(buf + pos + 1, buf + pos, len - pos);
			buf[pos] = c;
			len++;
			pos++;
			buf[len] = '\0';
		}
	}
}

int Linenoise::EditInsert(char c) {
	if (has_more_data) {
		render = false;
	}
	InsertCharacter(c);
	RefreshLine();
	return 0;
}

int Linenoise::EditInsertMulti(const char *c) {
	for (size_t pos = 0; c[pos]; pos++) {
		InsertCharacter(c[pos]);
	}
	RefreshLine();
	return 0;
}

size_t Linenoise::PrevChar() const {
	return duckdb::Utf8Proc::PreviousGraphemeCluster(buf, len, pos);
}

size_t Linenoise::NextChar() const {
	return duckdb::Utf8Proc::NextGraphemeCluster(buf, len, pos);
}

/* Move cursor on the left. */
void Linenoise::EditMoveLeft() {
	if (pos > 0) {
		pos = PrevChar();
		RefreshLine();
	}
}

/* Move cursor on the right. */
void Linenoise::EditMoveRight() {
	if (pos != len) {
		pos = NextChar();
		RefreshLine();
	}
}

bool Linenoise::IsWordBoundary(char c) {
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
void Linenoise::EditMoveWordLeft() {
	if (pos == 0) {
		return;
	}
	do {
		pos = PrevChar();
	} while (pos > 0 && !IsWordBoundary(buf[pos]));
	RefreshLine();
}

/* Move cursor on the right. */
void Linenoise::EditMoveWordRight() {
	if (pos == len) {
		return;
	}
	do {
		pos = NextChar();
	} while (pos != len && !IsWordBoundary(buf[pos]));
	RefreshLine();
}

/* Move cursor one row up. */
bool Linenoise::EditMoveRowUp() {
	if (!Terminal::IsMultiline()) {
		return false;
	}
	int rows, cols;
	int cursor_row, cursor_col;
	PositionToColAndRow(pos, cursor_row, cursor_col, rows, cols);
	if (cursor_row <= 1) {
		return false;
	}
	// we can move the cursor a line up
	lndebug("source pos %d", l->pos);
	lndebug("move from row %d to row %d", cursor_row, cursor_row - 1);
	cursor_row--;
	pos = ColAndRowToPosition(cursor_row, cursor_col);
	lndebug("new pos %d", l->pos);
	RefreshLine();
	return true;
}

/* Move cursor one row down. */
bool Linenoise::EditMoveRowDown() {
	if (!Terminal::IsMultiline()) {
		return false;
	}
	int rows, cols;
	int cursor_row, cursor_col;
	PositionToColAndRow(pos, cursor_row, cursor_col, rows, cols);
	if (cursor_row >= rows) {
		return false;
	}
	// we can move the cursor a line down
	lndebug("source pos %d", l->pos);
	lndebug("move from row %d to row %d", cursor_row, cursor_row + 1);
	cursor_row++;
	pos = ColAndRowToPosition(cursor_row, cursor_col);
	lndebug("new pos %d", l->pos);
	RefreshLine();
	return true;
}

/* Move cursor to the start of the line. */
void Linenoise::EditMoveHome() {
	if (pos != 0) {
		pos = 0;
		RefreshLine();
	}
}

/* Move cursor to the end of the line. */
void Linenoise::EditMoveEnd() {
	if (pos != len) {
		pos = len;
		RefreshLine();
	}
}

/* Substitute the currently edited line with the next or previous history
 * entry as specified by 'dir'. */
void Linenoise::EditHistoryNext(HistoryScrollDirection dir) {
	auto history_len = History::GetLength();
	if (history_len > 1) {
		/* Update the current history entry before to
		 * overwrite it with the next one. */
		History::Overwrite(history_len - 1 - history_index, buf);
		/* Show the new entry */
		if (dir == HistoryScrollDirection::LINENOISE_HISTORY_PREV) {
			// scroll back
			history_index++;
			if (history_index >= history_len) {
				history_index = history_len - 1;
				return;
			}
		} else {
			// scroll forwards
			if (history_index == 0) {
				return;
			}
			history_index--;
		}
		strncpy(buf, History::GetEntry(history_len - 1 - history_index), buflen);
		buf[buflen - 1] = '\0';
		len = pos = strlen(buf);
		if (Terminal::IsMultiline() && dir == HistoryScrollDirection::LINENOISE_HISTORY_NEXT) {
			pos = ColAndRowToPosition(1, len);
		}
		RefreshLine();
	}
}

/* Delete the character at the right of the cursor without altering the cursor
 * position. Basically this is what happens with the "Delete" keyboard key. */
void Linenoise::EditDelete() {
	if (len > 0 && pos < len) {
		size_t new_pos = NextChar();
		size_t char_sz = new_pos - pos;
		memmove(buf + pos, buf + new_pos, len - new_pos);
		len -= char_sz;
		buf[len] = '\0';
		RefreshLine();
	}
}

/* Backspace implementation. */
void Linenoise::EditBackspace() {
	if (pos > 0 && len > 0) {
		size_t new_pos = PrevChar();
		size_t char_sz = pos - new_pos;
		memmove(buf + new_pos, buf + pos, len - pos);
		len -= char_sz;
		pos = new_pos;
		buf[len] = '\0';
		RefreshLine();
	}
}

/* Delete the previous word, maintaining the cursor at the start of the
 * current word. */
void Linenoise::EditDeletePrevWord() {
	size_t old_pos = pos;
	size_t diff;

	while (pos > 0 && buf[pos - 1] == ' ') {
		pos--;
	}
	while (pos > 0 && buf[pos - 1] != ' ') {
		pos--;
	}
	diff = old_pos - pos;
	memmove(buf + pos, buf + old_pos, len - old_pos + 1);
	len -= diff;
	RefreshLine();
}

void Linenoise::CancelSearch() {
	search = false;
	search_buf = std::string();
	search_matches.clear();
	search_index = 0;
	RefreshLine();
}

char Linenoise::AcceptSearch(char nextCommand) {
	if (search_index < search_matches.size()) {
		// if there is a match - copy it into the buffer
		auto match = search_matches[search_index];
		auto history_entry = History::GetEntry(match.history_index);
		auto history_len = strlen(history_entry);
		memcpy(buf, history_entry, history_len);
		buf[history_len] = '\0';
		pos = match.match_end;
		len = history_len;
	}
	CancelSearch();
	return nextCommand;
}

void Linenoise::PerformSearch() {
	// we try to maintain the current match while searching
	size_t current_match = History::GetLength();
	if (search_index < search_matches.size()) {
		current_match = search_matches[search_index].history_index;
	}
	search_matches.clear();
	search_index = 0;
	if (search_buf.empty()) {
		return;
	}
	std::unordered_set<std::string> matches;
	auto lsearch = duckdb::StringUtil::Lower(search_buf);
	for (size_t i = History::GetLength(); i > 0; i--) {
		size_t history_index = i - 1;
		auto lhistory = duckdb::StringUtil::Lower(History::GetEntry(history_index));
		if (matches.find(lhistory) != matches.end()) {
			continue;
		}
		matches.insert(lhistory);
		auto entry = lhistory.find(lsearch);
		if (entry != duckdb::string::npos) {
			if (history_index == current_match) {
				search_index = search_matches.size();
			}
			searchMatch match;
			match.history_index = history_index;
			match.match_start = entry;
			match.match_end = entry + lsearch.size();
			search_matches.push_back(match);
		}
	}
}

void Linenoise::SearchPrev() {
	if (search_index > 0) {
		search_index--;
	} else if (search_matches.size() > 0) {
		search_index = search_matches.size() - 1;
	}
}

void Linenoise::SearchNext() {
	search_index += 1;
	if (search_index >= search_matches.size()) {
		search_index = 0;
	}
}

char Linenoise::Search(char c) {
	char seq[64];

	switch (c) {
	case 10:
	case ENTER: /* enter */
		// accept search and run
		return AcceptSearch(ENTER);
	case CTRL_R:
		// move to the next match index
		SearchNext();
		break;
	case ESC: /* escape sequence */
		/* Read the next two bytes representing the escape sequence.
		 * Use two calls to handle slow terminals returning the two
		 * chars at different times. */
		// note: in search mode we ignore almost all special commands
		if (read(ifd, seq, 1) == -1)
			break;
		if (seq[0] == ESC) {
			// double escape accepts search without any additional command
			return AcceptSearch(0);
		}
		if (seq[0] == 'b' || seq[0] == 'f') {
			break;
		}
		if (read(ifd, seq + 1, 1) == -1)
			break;

		/* ESC [ sequences. */
		if (seq[0] == '[') {
			if (seq[1] >= '0' && seq[1] <= '9') {
				/* Extended escape, read additional byte. */
				if (read(ifd, seq + 2, 1) == -1)
					break;
				if (seq[2] == '~') {
					switch (seq[1]) {
					case '1':
						return AcceptSearch(CTRL_A);
					case '4':
					case '8':
						return AcceptSearch(CTRL_E);
					default:
						break;
					}
				} else if (seq[2] == ';') {
					// read 2 extra bytes
					if (read(ifd, seq + 3, 2) == -1)
						break;
				}
			} else {
				switch (seq[1]) {
				case 'A': /* Up */
					SearchPrev();
					break;
				case 'B': /* Down */
					SearchNext();
					break;
				case 'D': /* Left */
					return AcceptSearch(CTRL_B);
				case 'C': /* Right */
					return AcceptSearch(CTRL_F);
				case 'H': /* Home */
					return AcceptSearch(CTRL_A);
				case 'F': /* End*/
					return AcceptSearch(CTRL_E);
				default:
					break;
				}
			}
		}
		/* ESC O sequences. */
		else if (seq[0] == 'O') {
			switch (seq[1]) {
			case 'H': /* Home */
				return AcceptSearch(CTRL_A);
			case 'F': /* End*/
				return AcceptSearch(CTRL_E);
			default:
				break;
			}
		}
		break;
	case CTRL_A: // accept search, move to start of line
		return AcceptSearch(CTRL_A);
	case '\t':
	case CTRL_E: // accept search - move to end of line
		return AcceptSearch(CTRL_E);
	case CTRL_B: // accept search - move cursor left
		return AcceptSearch(CTRL_B);
	case CTRL_F: // accept search - move cursor right
		return AcceptSearch(CTRL_F);
	case CTRL_T: // accept search: swap character
		return AcceptSearch(CTRL_T);
	case CTRL_U: // accept search, clear buffer
		return AcceptSearch(CTRL_U);
	case CTRL_K: // accept search, clear after cursor
		return AcceptSearch(CTRL_K);
	case CTRL_D: // accept search, delete a character
		return AcceptSearch(CTRL_D);
	case CTRL_L:
		linenoiseClearScreen();
		break;
	case CTRL_P:
		SearchPrev();
		break;
	case CTRL_N:
		SearchNext();
		break;
	case CTRL_C:
	case CTRL_G:
		// abort search
		CancelSearch();
		return 0;
	case BACKSPACE: /* backspace */
	case 8:         /* ctrl-h */
	case CTRL_W:    /* ctrl-w */
		// remove trailing UTF-8 bytes (if any)
		while (!search_buf.empty() && ((search_buf.back() & 0xc0) == 0x80)) {
			search_buf.pop_back();
		}
		// finally remove the first UTF-8 byte
		if (!search_buf.empty()) {
			search_buf.pop_back();
		}
		PerformSearch();
		break;
	default:
		// add input to search buffer
		search_buf += c;
		// perform the search
		PerformSearch();
		break;
	}
	RefreshSearch();
	return 0;
}

bool Linenoise::AllWhitespace(const char *z) {
	for (; *z; z++) {
		if (isspace((unsigned char)z[0]))
			continue;
		if (*z == '/' && z[1] == '*') {
			z += 2;
			while (*z && (*z != '*' || z[1] != '/')) {
				z++;
			}
			if (*z == 0) {
				return false;
			}
			z++;
			continue;
		}
		if (*z == '-' && z[1] == '-') {
			z += 2;
			while (*z && *z != '\n') {
				z++;
			}
			if (*z == 0) {
				return true;
			}
			continue;
		}
		return false;
	}
	return true;
}

Linenoise::Linenoise(int stdin_fd, int stdout_fd, char *buf, size_t buflen, const char *prompt)
    : ifd(stdin_fd), ofd(stdout_fd), buf(buf), buflen(buflen), prompt(prompt), plen(strlen(prompt)) {
	pos = 0;
	old_cursor_rows = 1;
	len = 0;
	ws = Terminal::GetTerminalSize();
	maxrows = 0;
	history_index = 0;
	y_scroll = 0;
	clear_screen = false;
	search = false;
	has_more_data = false;
	render = true;
	continuation_markers = true;

	/* Buffer starts empty. */
	buf[0] = '\0';
	buflen--; /* Make sure there is always space for the nulterm */
}

/* This function is the core of the line editing capability of linenoise.
 * It expects 'fd' to be already in "raw mode" so that every key pressed
 * will be returned ASAP to read().
 *
 * The resulting string is put into 'buf' when the user type enter, or
 * when ctrl+d is typed.
 *
 * The function returns the length of the current buffer. */
int Linenoise::Edit() {
	/* The latest history entry is always our current buffer, that
	 * initially is just an empty string. */
	History::Add("");

	if (write(ofd, prompt, plen) == -1) {
		return -1;
	}
	while (true) {
		char c;
		int nread;
		char seq[5];

		nread = read(ifd, &c, 1);
		if (nread <= 0) {
			return len;
		}
		has_more_data = Terminal::HasMoreData(ifd);
		render = true;
		if (Terminal::IsMultiline() && !has_more_data) {
			TerminalSize new_size = Terminal::GetTerminalSize();
			if (new_size.ws_col != ws.ws_col || new_size.ws_row != ws.ws_row) {
				// terminal resize! re-compute max lines
				ws = new_size;
				int rows, cols;
				int cursor_row, cursor_col;
				PositionToColAndRow(pos, cursor_row, cursor_col, rows, cols);
				old_cursor_rows = cursor_row;
				maxrows = rows;
			}
		}

		if (search) {
			char ret = Search(c);
			if (search || ret == '\0') {
				// still searching - continue searching
				continue;
			}
			// run subsequent command
			c = ret;
		}

		/* Only autocomplete when the callback is set. It returns < 0 when
		 * there was an error reading from fd. Otherwise it will return the
		 * character that should be handled next. */
		if (c == TAB && completionCallback != NULL) {
			if (has_more_data) {
				// if there is more data, this tab character was added as part of copy-pasting data
				continue;
			}
			c = CompleteLine();
			/* Return on errors */
			if (c < 0) {
				return len;
			}
			/* Read next character when 0 */
			if (c == 0) {
				continue;
			}
		}

		lndebug("%d\n", (int)c);
		switch (c) {
		case 10:
		case ENTER: /* enter */
			if (Terminal::IsMultiline() && len > 0) {
				// check if this forms a complete SQL statement or not
				buf[len] = '\0';
				if (buf[0] != '.' && !AllWhitespace(buf) && !sqlite3_complete(buf)) {
					// not a complete SQL statement yet! continuation
					// insert "\r\n"
					if (EditInsertMulti("\r\n")) {
						return -1;
					}
					break;
				}
			}
			// final refresh before returning control to the shell
			continuation_markers = false;
			History::RemoveLastEntry();
			if (Terminal::IsMultiline()) {
				if (pos == len) {
					// already at the end - only refresh
					RefreshLine();
				} else {
					EditMoveEnd();
				}
			}
			if (hintsCallback) {
				/* Force a refresh without hints to leave the previous
				 * line as the user typed it after a newline. */
				linenoiseHintsCallback *hc = hintsCallback;
				hintsCallback = NULL;
				RefreshLine();
				hintsCallback = hc;
			}
			return (int)len;
		case CTRL_G:
		case CTRL_C: /* ctrl-c */ {
			if (Terminal::IsMultiline()) {
				EditMoveEnd();
			}
			buf[0] = '\3';
			// we keep track of whether or not the line was empty by writing \3 to the second position of the line
			// this is because at a higher level we might want to know if we pressed ctrl c to clear the line
			// or to exit the process
			if (len > 0) {
				buf[1] = '\3';
				buf[2] = '\0';
				pos = 2;
				len = 2;
			} else {
				buf[1] = '\0';
				pos = 1;
				len = 1;
			}
			return (int)len;
		}
		case BACKSPACE: /* backspace */
		case 8:         /* ctrl-h */
			EditBackspace();
			break;
		case CTRL_D: /* ctrl-d, remove char at right of cursor, or if the
		        line is empty, act as end-of-file. */
			if (len > 0) {
				EditDelete();
			} else {
				History::RemoveLastEntry();
				return -1;
			}
			break;
		case CTRL_Z: /* ctrl-z, suspends shell */
			Terminal::DisableRawMode();
			raise(SIGTSTP);
			Terminal::EnableRawMode();
			RefreshLine();
			break;
		case CTRL_T: /* ctrl-t, swaps current character with previous. */
			if (pos > 0 && pos < len) {
				char temp_buffer[128];
				int prev_pos = PrevChar();
				int next_pos = NextChar();
				int prev_char_size = pos - prev_pos;
				int cur_char_size = next_pos - pos;
				memcpy(temp_buffer, buf + prev_pos, prev_char_size);
				memmove(buf + prev_pos, buf + pos, cur_char_size);
				memcpy(buf + prev_pos + cur_char_size, temp_buffer, prev_char_size);
				pos = next_pos;
				RefreshLine();
			}
			break;
		case CTRL_B: /* ctrl-b */
			EditMoveLeft();
			break;
		case CTRL_F: /* ctrl-f */
			EditMoveRight();
			break;
		case CTRL_P: /* ctrl-p */
			EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_PREV);
			break;
		case CTRL_N: /* ctrl-n */
			EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_NEXT);
			break;
		case CTRL_R: /* ctrl-r */ {
			// initiate reverse search
			search = true;
			search_buf = std::string();
			search_matches.clear();
			search_index = 0;
			RefreshSearch();
			break;
		}
		case ESC: /* escape sequence */
			/* Read the next two bytes representing the escape sequence.
			 * Use two calls to handle slow terminals returning the two
			 * chars at different times. */
			if (read(ifd, seq, 1) == -1)
				break;
			if (seq[0] == 'b') {
				EditMoveWordLeft();
				break;
			} else if (seq[0] == 'f') {
				EditMoveWordRight();
				break;
			}
			// lndebug("seq0: %d\n", seq[0]);
			if (read(ifd, seq + 1, 1) == -1) {
				break;
			}
			// lndebug("seq1: %d\n", seq[1]);

			/* ESC [ sequences. */
			if (seq[0] == '[') {
				if (seq[1] >= '0' && seq[1] <= '9') {
					/* Extended escape, read additional byte. */
					if (read(ifd, seq + 2, 1) == -1)
						break;
					if (seq[2] == '~') {
						switch (seq[1]) {
						case '1':
							EditMoveHome();
							break;
						case '3': /* Delete key. */
							EditDelete();
							break;
						case '4':
							EditMoveEnd();
							break;
						case '8':
							EditMoveEnd();
							break;
						default:
							lndebug("unrecognized escape sequence (~) %d", seq[1]);
							break;
						}
					} else if (seq[2] == ';') {
						// read 2 extra bytes
						if (read(ifd, seq + 3, 2) == -1)
							break;
						if (memcmp(seq, "[1;5C", 5) == 0) {
							// [1;5C: move word right
							EditMoveWordRight();
						} else if (memcmp(seq, "[1;5D", 5) == 0) {
							// [1;5D: move word left
							EditMoveWordLeft();
						} else {
							lndebug("unrecognized escape sequence (;) %d", seq[1]);
						}
					} else if (seq[1] == '5' && seq[2] == 'C') {
						EditMoveWordRight();
					} else if (seq[1] == '5' && seq[2] == 'D') {
						EditMoveWordLeft();
					}
				} else {
					switch (seq[1]) {
					case 'A': /* Up */
						if (EditMoveRowUp()) {
							break;
						}
						EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_PREV);
						break;
					case 'B': /* Down */
						if (EditMoveRowDown()) {
							break;
						}
						EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_NEXT);
						break;
					case 'C': /* Right */
						EditMoveRight();
						break;
					case 'D': /* Left */
						EditMoveLeft();
						break;
					case 'H': /* Home */
						EditMoveHome();
						break;
					case 'F': /* End*/
						EditMoveEnd();
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
					EditMoveHome();
					break;
				case 'F': /* End*/
					EditMoveEnd();
					break;
				case 'c':
					EditMoveWordRight();
					break;
				case 'd':
					EditMoveWordLeft();
					break;
				default:
					lndebug("unrecognized escape sequence (O) %d", seq[1]);
					break;
				}
			}
			break;
		case CTRL_U: /* Ctrl+u, delete the whole line. */
			buf[0] = '\0';
			pos = len = 0;
			RefreshLine();
			break;
		case CTRL_K: /* Ctrl+k, delete from current to end of line. */
			buf[pos] = '\0';
			len = pos;
			RefreshLine();
			break;
		case CTRL_A: /* Ctrl+a, go to the start of the line */
			EditMoveHome();
			break;
		case CTRL_E: /* ctrl+e, go to the end of the line */
			EditMoveEnd();
			break;
		case CTRL_L: /* ctrl+l, clear screen */
			linenoiseClearScreen();
			clear_screen = true;
			RefreshLine();
			break;
		case CTRL_W: /* ctrl+w, delete previous word */
			EditDeletePrevWord();
			break;
		default: {
			if (EditInsert(c)) {
				return -1;
			}
			break;
		}
		}
	}
	return len;
}

} // namespace duckdb
