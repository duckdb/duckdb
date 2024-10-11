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

#ifdef LINENOISE_EDITOR
#if defined(WIN32) || defined(__CYGWIN__)
#define DEFAULT_EDITOR "notepad.exe"
#else
#define DEFAULT_EDITOR "vi"
#endif
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

TabCompletion Linenoise::TabComplete() const {
	TabCompletion result;
	if (!completionCallback) {
		return result;
	}
	linenoiseCompletions lc;
	lc.cvec = nullptr;
	lc.len = 0;
	// complete based on the cursor position
	auto prev_char = buf[pos];
	buf[pos] = '\0';
	completionCallback(buf, &lc);
	buf[pos] = prev_char;
	result.completions.reserve(lc.len);
	for (idx_t i = 0; i < lc.len; i++) {
		Completion c;
		c.completion = lc.cvec[i];
		c.cursor_pos = c.completion.size();
		c.completion += buf + pos;
		result.completions.emplace_back(std::move(c));
	}
	freeCompletions(&lc);
	return result;
}
/* This is an helper function for linenoiseEdit() and is called when the
 * user types the <tab> key in order to complete the string currently in the
 * input.
 *
 * The state of the editing is encapsulated into the pointed linenoiseState
 * structure as described in the structure definition. */
int Linenoise::CompleteLine(EscapeSequence &current_sequence) {
	int nread, nwritten;
	char c = 0;

	auto completion_list = TabComplete();
	auto &completions = completion_list.completions;
	if (completions.empty()) {
		Terminal::Beep();
	} else {
		bool stop = false;
		bool accept_completion = false;
		idx_t i = 0;

		while (!stop) {
			/* Show completion or original buffer */
			if (i < completions.size()) {
				Linenoise saved = *this;

				len = completions[i].completion.size();
				pos = completions[i].cursor_pos;
				buf = (char *)completions[i].completion.c_str();
				RefreshLine();
				len = saved.len;
				pos = saved.pos;
				buf = saved.buf;
			} else {
				RefreshLine();
			}

			nread = read(ifd, &c, 1);
			if (nread <= 0) {
				return -1;
			}

			Linenoise::Log("\nComplete Character %d\n", (int)c);
			switch (c) {
			case TAB: /* tab */
				i = (i + 1) % (completions.size() + 1);
				if (i == completions.size()) {
					Terminal::Beep();
				}
				break;
			case ESC: { /* escape */
				auto escape = Terminal::ReadEscapeSequence(ifd);
				switch (escape) {
				case EscapeSequence::SHIFT_TAB:
					// shift-tab: move backwards
					if (i == 0) {
						Terminal::Beep();
					} else {
						i--;
					}
					break;
				case EscapeSequence::ESCAPE:
					/* Re-show original buffer */
					if (i < completions.size()) {
						RefreshLine();
					}
					current_sequence = escape;
					stop = true;
					break;
				default:
					current_sequence = escape;
					accept_completion = true;
					stop = true;
					break;
				}
				break;
			}
			default:
				accept_completion = true;
				stop = true;
				break;
			}
			if (stop && accept_completion && i < completions.size()) {
				/* Update buffer and return */
				if (i < completions.size()) {
					nwritten = snprintf(buf, buflen, "%s", completions[i].completion.c_str());
					pos = completions[i].cursor_pos;
					len = nwritten;
				}
			}
		}
	}
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
	} else if (strcmp(azArg[0], "render_errors") == 0) {
		if (nArg == 2) {
			if (strcmp(azArg[1], "off") == 0 || strcmp(azArg[1], "0") == 0) {
				Linenoise::DisableErrorRendering();
				return 1;
			} else if (strcmp(azArg[1], "on") == 0 || strcmp(azArg[1], "1") == 0) {
				Linenoise::EnableErrorRendering();
				return 1;
			}
		}
		*out_error = "Expected usage: .render_errors [off|on]";
		return 1;
	} else if (strcmp(azArg[0], "render_completion") == 0) {
		if (nArg == 2) {
			if (strcmp(azArg[1], "off") == 0 || strcmp(azArg[1], "0") == 0) {
				Linenoise::DisableCompletionRendering();
				return 1;
			} else if (strcmp(azArg[1], "on") == 0 || strcmp(azArg[1], "1") == 0) {
				Linenoise::EnableCompletionRendering();
				return 1;
			}
		}
		*out_error = "Expected usage: .render_completion [off|on]";
		return 1;
	} else if (strcmp(azArg[0], "keyword") == 0 || strcmp(azArg[0], "constant") == 0 ||
	           strcmp(azArg[0], "comment") == 0 || strcmp(azArg[0], "error") == 0 || strcmp(azArg[0], "cont") == 0 ||
	           strcmp(azArg[0], "cont_sel") == 0) {
		if (nArg == 2) {
			const char *option = Highlighting::GetColorOption(azArg[1]);
			if (option) {
				HighlightingType type;
				if (strcmp(azArg[0], "keyword") == 0) {
					type = HighlightingType::KEYWORD;
				} else if (strcmp(azArg[0], "constant") == 0) {
					type = HighlightingType::CONSTANT;
				} else if (strcmp(azArg[0], "comment") == 0) {
					type = HighlightingType::COMMENT;
				} else if (strcmp(azArg[0], "error") == 0) {
					type = HighlightingType::ERROR;
				} else if (strcmp(azArg[0], "cont") == 0) {
					type = HighlightingType::CONTINUATION;
				} else {
					type = HighlightingType::CONTINUATION_SELECTED;
				}
				Highlighting::SetHighlightingColor(type, option);
				return 1;
			}
		}
		*out_error = "Expected usage: .[keyword|constant|comment|error|cont|cont_sel] "
		             "[red|green|yellow|blue|magenta|cyan|white|brightblack|brightred|brightgreen|brightyellow|"
		             "brightblue|brightmagenta|brightcyan|brightwhite]";
		return 1;
	} else if (strcmp(azArg[0], "keywordcode") == 0 || strcmp(azArg[0], "constantcode") == 0 ||
	           strcmp(azArg[0], "commentcode") == 0 || strcmp(azArg[0], "errorcode") == 0 ||
	           strcmp(azArg[0], "contcode") == 0 || strcmp(azArg[0], "cont_selcode") == 0) {
		if (nArg == 2) {
			HighlightingType type;
			if (strcmp(azArg[0], "keywordcode") == 0) {
				type = HighlightingType::KEYWORD;
			} else if (strcmp(azArg[0], "constantcode") == 0) {
				type = HighlightingType::CONSTANT;
			} else if (strcmp(azArg[0], "commentcode") == 0) {
				type = HighlightingType::COMMENT;
			} else if (strcmp(azArg[0], "errorcode") == 0) {
				type = HighlightingType::ERROR;
			} else if (strcmp(azArg[0], "contcode") == 0) {
				type = HighlightingType::CONTINUATION;
			} else {
				type = HighlightingType::CONTINUATION_SELECTED;
			}
			Highlighting::SetHighlightingColor(type, azArg[1]);
			return 1;
		}
		*out_error =
		    "Expected usage: .[keywordcode|constantcode|commentcode|errorcode|contcode|cont_selcode] [terminal_code]";
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
	insert = true;
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
	Linenoise::Log("source pos %d", pos);
	Linenoise::Log("move from row %d to row %d", cursor_row, cursor_row - 1);
	cursor_row--;
	pos = ColAndRowToPosition(cursor_row, cursor_col);
	Linenoise::Log("new pos %d", pos);
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
	Linenoise::Log("source pos %d", pos);
	Linenoise::Log("move from row %d to row %d", cursor_row, cursor_row + 1);
	cursor_row++;
	pos = ColAndRowToPosition(cursor_row, cursor_col);
	Linenoise::Log("new pos %d", pos);
	RefreshLine();
	return true;
}

/* Move cursor to the start of the buffer. */
void Linenoise::EditMoveHome() {
	if (pos != 0) {
		pos = 0;
		RefreshLine();
	}
}

/* Move cursor to the end of the buffer. */
void Linenoise::EditMoveEnd() {
	if (pos != len) {
		pos = len;
		RefreshLine();
	}
}

/* Move cursor to the start of the line. */
void Linenoise::EditMoveStartOfLine() {
	while (pos > 0 && buf[pos] != '\n') {
		pos--;
	}
	RefreshLine();
}

/* Move cursor to the end of the line. */
void Linenoise::EditMoveEndOfLine() {
	while (pos < len && buf[pos + 1] != '\n') {
		pos++;
	}
	RefreshLine();
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
		switch (dir) {
		case HistoryScrollDirection::LINENOISE_HISTORY_PREV:
			// scroll back
			history_index++;
			if (history_index >= history_len) {
				history_index = history_len - 1;
				return;
			}
			break;
		case HistoryScrollDirection::LINENOISE_HISTORY_NEXT:
			// scroll forwards
			if (history_index == 0) {
				return;
			}
			history_index--;
			break;
		case HistoryScrollDirection::LINENOISE_HISTORY_END:
			history_index = 0;
			break;
		case HistoryScrollDirection::LINENOISE_HISTORY_START:
			history_index = history_len - 1;
			break;
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

bool Linenoise::IsSpace(char c) {
	switch (c) {
	case ' ':
	case '\r':
	case '\n':
	case '\t':
		return true;
	default:
		return false;
	}
}

/* Delete the previous word, maintaining the cursor at the start of the
 * current word. */
void Linenoise::EditDeletePrevWord() {
	size_t old_pos = pos;
	size_t diff;

	while (pos > 0 && IsSpace(buf[pos - 1])) {
		pos--;
	}
	while (pos > 0 && !IsSpace(buf[pos - 1])) {
		pos--;
	}
	diff = old_pos - pos;
	memmove(buf + pos, buf + old_pos, len - old_pos + 1);
	len -= diff;
	RefreshLine();
}

/* Delete the next word */
void Linenoise::EditDeleteNextWord() {
	size_t next_pos = pos;
	size_t diff;

	while (next_pos < len && IsSpace(buf[next_pos])) {
		next_pos++;
	}
	while (next_pos < len && !IsSpace(buf[next_pos])) {
		next_pos++;
	}
	diff = next_pos - pos;
	memmove(buf + pos, buf + next_pos, len - next_pos + 1);
	len -= diff;
	RefreshLine();
}

void Linenoise::EditRemoveSpaces() {
	size_t start_pos = pos;
	size_t end_pos = pos;
	size_t diff;

	while (start_pos > 0 && buf[start_pos - 1] == ' ') {
		start_pos--;
	}
	while (end_pos < len && buf[end_pos] == ' ') {
		end_pos++;
	}
	diff = end_pos - start_pos;
	memmove(buf + start_pos, buf + end_pos, len - end_pos + 1);
	len -= diff;
	pos = start_pos;
	RefreshLine();
}

void Linenoise::EditSwapCharacter() {
	if (pos == 0 || len < 2) {
		return;
	}
	char temp_buffer[128];
	if (pos + 1 > len) {
		pos = PrevChar();
	}
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

void Linenoise::EditSwapWord() {
	idx_t current_pos = pos;
	idx_t left_word_start, left_word_end;
	idx_t right_word_start, right_word_end;

	// move to the end of the right word
	idx_t end_pos = pos;
	while (end_pos < len && IsSpace(buf[end_pos])) {
		end_pos++;
	}
	while (end_pos < len && !IsSpace(buf[end_pos])) {
		end_pos++;
	}
	current_pos = end_pos;
	while (current_pos > 0 && IsSpace(buf[current_pos - 1])) {
		current_pos--;
	}
	right_word_end = current_pos;
	// move to the start of the right word
	while (current_pos > 0 && !IsSpace(buf[current_pos - 1])) {
		current_pos--;
	}
	right_word_start = current_pos;

	// move to the left of the left word
	while (current_pos > 0 && IsSpace(buf[current_pos - 1])) {
		current_pos--;
	}
	left_word_end = current_pos;
	while (current_pos > 0 && !IsSpace(buf[current_pos - 1])) {
		current_pos--;
	}
	left_word_start = current_pos;
	if (left_word_start == right_word_start) {
		// no words to swap
		return;
	}
	idx_t left_len = left_word_end - left_word_start;
	idx_t right_len = right_word_end - right_word_start;
	if (left_len == 0 || right_len == 0) {
		// one of the words has no length
		return;
	}
	// now we need to swap [LEFT][RIGHT] to [RIGHT][LEFT]
	// there's certainly some fancy way of doing this in-place
	// but this is way easier
	string left_word(buf + left_word_start, left_len);
	string right_word(buf + right_word_start, right_len);
	memset(buf + left_word_start, ' ', end_pos - left_word_start);
	memcpy(buf + left_word_start, right_word.c_str(), right_word.size());
	memcpy(buf + end_pos - left_len, left_word.c_str(), left_word.size());
	pos = end_pos;
	RefreshLine();
}

void Linenoise::EditDeleteAll() {
	buf[0] = '\0';
	pos = len = 0;
	RefreshLine();
}

void Linenoise::EditCapitalizeNextWord(Capitalization capitalization) {
	size_t next_pos = pos;

	// find the next word
	while (next_pos < len && IsSpace(buf[next_pos])) {
		next_pos++;
	}
	bool first_char = true;
	while (next_pos < len && !IsSpace(buf[next_pos])) {
		switch (capitalization) {
		case Capitalization::CAPITALIZE: {
			if (first_char) {
				first_char = false;
				if (buf[next_pos] >= 'a' && buf[next_pos] <= 'z') {
					buf[next_pos] -= 'a' - 'A';
				}
			}
			break;
		}
		case Capitalization::LOWERCASE: {
			if (buf[next_pos] >= 'A' && buf[next_pos] <= 'Z') {
				buf[next_pos] += 'a' - 'A';
			}
			break;
		}
		case Capitalization::UPPERCASE: {
			if (buf[next_pos] >= 'a' && buf[next_pos] <= 'z') {
				buf[next_pos] -= 'a' - 'A';
			}
			break;
		}
		default:
			break;
		}
		next_pos++;
	}
	pos = next_pos;
	RefreshLine();
}

void Linenoise::StartSearch() {
	// initiate reverse search
	search = true;
	search_buf = std::string();
	search_matches.clear();
	search_index = 0;
	RefreshSearch();
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
	switch (c) {
	case CTRL_J:
	case ENTER: /* enter */
		// accept search and run
		return AcceptSearch(ENTER);
	case CTRL_R:
	case CTRL_S:
		// move to the next match index
		SearchNext();
		break;
	case ESC: /* escape sequence */ {
		auto escape = Terminal::ReadEscapeSequence(ifd);
		switch (escape) {
		case EscapeSequence::ESCAPE:
			// double escape accepts search without any additional command
			return AcceptSearch(0);
		case EscapeSequence::UP:
			SearchPrev();
			break;
		case EscapeSequence::DOWN:
			SearchNext();
			break;
		case EscapeSequence::HOME:
			return AcceptSearch(CTRL_A);
		case EscapeSequence::END:
			return AcceptSearch(CTRL_E);
		case EscapeSequence::LEFT:
			return AcceptSearch(CTRL_B);
		case EscapeSequence::RIGHT:
			return AcceptSearch(CTRL_F);
		default:
			break;
		}
		break;
	}
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
	case CTRL_H:    /* ctrl-h */
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
	insert = false;
	search_index = 0;

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
		EscapeSequence current_sequence = EscapeSequence::INVALID;
		char c;
		int nread;

		nread = read(ifd, &c, 1);
		if (nread <= 0) {
			return len;
		}
		has_more_data = Terminal::HasMoreData(ifd);
		render = true;
		insert = false;
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
				// instead insert some spaces
				if (EditInsertMulti("    ")) {
					return -1;
				}
				continue;
			}
			c = CompleteLine(current_sequence);
			/* Return on errors */
			if (c < 0) {
				return len;
			}
			/* Read next character when 0 */
			if (c == 0) {
				continue;
			}
		}

		Linenoise::Log("%d\n", (int)c);
		switch (c) {
		case CTRL_J:
		case ENTER: { /* enter */
#ifdef LINENOISE_EDITOR
			if (len > 0) {
				// check if this contains ".edit"

				// scroll back to last newline
				idx_t begin_pos;
				for (begin_pos = len; begin_pos > 0 && buf[begin_pos - 1] != '\n'; begin_pos--) {
				}
				// check if line is ".edit"
				bool open_editor = false;
				if (begin_pos + 5 == len && memcmp(buf + begin_pos, ".edit", 5) == 0) {
					open_editor = true;
				}
				// check if line is "\\e"
				if (begin_pos + 2 == len && memcmp(buf + begin_pos, "\\e", 2) == 0) {
					open_editor = true;
				}
				if (open_editor) {
					// .edit
					// clear the buffer and open the editor
					pos = len = begin_pos;
					if (!EditBufferWithEditor(nullptr)) {
						// failed to edit - refresh the removal of ".edit" / "\e"
						RefreshLine();
						break;
					}
				}
			}
#endif
			if (Terminal::IsMultiline() && len > 0) {
				// check if this forms a complete SQL statement or not
				buf[len] = '\0';
				if (buf[0] != '.' && !AllWhitespace(buf) && !sqlite3_complete(buf)) {
					// not a complete SQL statement yet! continuation
					// insert "\r\n" at the end
					pos = len;
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
			// rewrite \r\n to \n
			idx_t new_len = 0;
			for (idx_t i = 0; i < len; i++) {
				if (buf[i] == '\r' && buf[i + 1] == '\n') {
					continue;
				}
				buf[new_len++] = buf[i];
			}
			buf[new_len] = '\0';
			return (int)new_len;
		}
		case CTRL_O:
		case CTRL_G:
		case CTRL_C: /* ctrl-c */ {
			if (Terminal::IsMultiline()) {
				continuation_markers = false;
				// force a refresh by setting pos to 0
				pos = 0;
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
		case CTRL_H:    /* ctrl-h */
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
			EditSwapCharacter();
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
		case CTRL_S:
		case CTRL_R: /* ctrl-r */ {
			StartSearch();
			break;
		}
		case ESC: /* escape sequence */ {
			EscapeSequence escape;
			if (current_sequence == EscapeSequence::INVALID) {
				// read escape sequence
				escape = Terminal::ReadEscapeSequence(ifd);
			} else {
				// use stored sequence
				escape = current_sequence;
				current_sequence = EscapeSequence::INVALID;
			}
			switch (escape) {
			case EscapeSequence::ALT_LEFT_ARROW:
				EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_START);
				break;
			case EscapeSequence::ALT_RIGHT_ARROW:
				EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_END);
				break;
			case EscapeSequence::CTRL_MOVE_BACKWARDS:
			case EscapeSequence::ALT_B:
				EditMoveWordLeft();
				break;
			case EscapeSequence::CTRL_MOVE_FORWARDS:
			case EscapeSequence::ALT_F:
				EditMoveWordRight();
				break;
			case EscapeSequence::ALT_D:
				EditDeleteNextWord();
				break;
			case EscapeSequence::ALT_C:
				EditCapitalizeNextWord(Capitalization::CAPITALIZE);
				break;
			case EscapeSequence::ALT_L:
				EditCapitalizeNextWord(Capitalization::LOWERCASE);
				break;
			case EscapeSequence::ALT_N:
			case EscapeSequence::ALT_P:
				StartSearch();
				break;
			case EscapeSequence::ALT_T:
				EditSwapWord();
				break;
			case EscapeSequence::ALT_U:
				EditCapitalizeNextWord(Capitalization::UPPERCASE);
				break;
			case EscapeSequence::ALT_R:
				EditDeleteAll();
				break;
			case EscapeSequence::ALT_BACKSPACE:
				EditDeletePrevWord();
				break;
			case EscapeSequence::ALT_BACKSLASH:
				EditRemoveSpaces();
				break;
			case EscapeSequence::HOME:
				EditMoveHome();
				break;
			case EscapeSequence::END:
				EditMoveEnd();
				break;
			case EscapeSequence::UP:
				if (EditMoveRowUp()) {
					break;
				}
				EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_PREV);
				break;
			case EscapeSequence::DOWN:
				if (EditMoveRowDown()) {
					break;
				}
				EditHistoryNext(HistoryScrollDirection::LINENOISE_HISTORY_NEXT);
				break;
			case EscapeSequence::RIGHT:
				EditMoveRight();
				break;
			case EscapeSequence::LEFT:
				EditMoveLeft();
				break;
			case EscapeSequence::DELETE:
				EditDelete();
				break;
			default:
				Linenoise::Log("Unrecognized escape\n");
				break;
			}
			break;
		}
		case CTRL_U: /* Ctrl+u, delete the whole line. */
			EditDeleteAll();
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
		case CTRL_X: /* ctrl+x, insert newline */
			EditInsertMulti("\r\n");
			break;
		case CTRL_Y:
			// unsupported
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

#ifdef LINENOISE_LOGGING
void Linenoise::LogMessageRecursive(const string &msg, std::vector<ExceptionFormatValue> &values) {
	static FILE *lndebug_fp = NULL;
	if (!lndebug_fp) {
		lndebug_fp = fopen("/tmp/lndebug.txt", "a");
	}
	auto log_message = Exception::ConstructMessageRecursive(msg, values);
	fprintf(lndebug_fp, "%s", log_message.c_str());
	fflush(lndebug_fp);
}
#endif

void Linenoise::LogTokens(const vector<highlightToken> &tokens) {
#ifdef LINENOISE_LOGGING
	for (auto &token : tokens) {
		const char *token_type_name = "";
		switch (token.type) {
		case tokenType::TOKEN_IDENTIFIER:
			token_type_name = "IDENTIFIER";
			break;
		case tokenType::TOKEN_NUMERIC_CONSTANT:
			token_type_name = "NUMERIC_CONSTANT";
			break;
		case tokenType::TOKEN_STRING_CONSTANT:
			token_type_name = "STRING_CONSTANT";
			break;
		case tokenType::TOKEN_OPERATOR:
			token_type_name = "OPERATOR";
			break;
		case tokenType::TOKEN_KEYWORD:
			token_type_name = "KEYWORD";
			break;
		case tokenType::TOKEN_COMMENT:
			token_type_name = "COMMENT";
			break;
		case tokenType::TOKEN_CONTINUATION:
			token_type_name = "CONTINUATION";
			break;
		case tokenType::TOKEN_CONTINUATION_SELECTED:
			token_type_name = "CONTINUATION_SELECTED";
			break;
		case tokenType::TOKEN_BRACKET:
			token_type_name = "BRACKET";
			break;
		case tokenType::TOKEN_ERROR:
			token_type_name = "ERROR";
			break;
		default:
			break;
		}
		Linenoise::Log("Token at position %d with type %s\n", token.start, token_type_name);
	}
#endif
}

#ifdef LINENOISE_EDITOR
// .edit functionality - code adopted from psql

bool Linenoise::EditFileWithEditor(const string &file_name, const char *editor) {
	/* Find an editor to use */
	if (!editor) {
		editor = getenv("DUCKDB_EDITOR");
	}
	if (!editor) {
		editor = getenv("EDITOR");
	}
	if (!editor) {
		editor = getenv("VISUAL");
	}
	if (!editor) {
		editor = DEFAULT_EDITOR;
	}

	/*
	 * On Unix the EDITOR value should *not* be quoted, since it might include
	 * switches, eg, EDITOR="pico -t"; it's up to the user to put quotes in it
	 * if necessary.  But this policy is not very workable on Windows, due to
	 * severe brain damage in their command shell plus the fact that standard
	 * program paths include spaces.
	 */
	string command;
#ifndef WIN32
	command = "exec " + string(editor) + " '" + file_name + "'";
#else
	command = "\"" + string(editor) + "\" \"" + file_name + "\"";
#endif
	int result = system(command.c_str());
	if (result == -1) {
		Log("could not start editor \"%s\"\n", editor);
	} else if (result == 127) {
		Log("could not start /bin/sh\n");
	}
	return result == 0;
}

bool Linenoise::EditBufferWithEditor(const char *editor) {
	/* make a temp file to edit */
#ifndef WIN32
	const char *tmpdir = getenv("TMPDIR");
	if (!tmpdir) {
		tmpdir = "/tmp";
	}
#else
	char tmpdir[MAX_PATH_LENGTH];
	int ret;

	ret = GetTempPath(MAX_PATH_LENGTH, tmpdir);
	if (ret == 0 || ret > MAX_PATH_LENGTH) {
		Log("cannot locate temporary directory: %s", !ret ? strerror(errno) : "");
		return false;
	}
#endif
	string temporary_file_name;
#ifndef WIN32
	temporary_file_name = string(tmpdir) + "/duckdb.edit." + std::to_string(getpid()) + ".sql";
#else
	temporary_file_name = string(tmpdir) + "duckdb.edit." + std::to_string(getpid()) + ".sql";
#endif

	FILE *f = fopen(temporary_file_name.c_str(), "w+");
	if (!f) {
		Log("could not open temporary file \"%s\": %s\n", temporary_file_name, strerror(errno));
		Terminal::Beep();
		return false;
	}

	// edit the current buffer by default
	const char *write_buffer = buf;
	idx_t write_len = len;
	if (write_len == 0) {
		// if the current buffer is empty we are typing ".edit" as the first command
		// edit the previous history entry
		auto edit_index = History::GetLength();
		if (edit_index >= 2) {
			auto history_entry = History::GetEntry(edit_index - 2);
			if (history_entry) {
				write_buffer = history_entry;
				write_len = strlen(history_entry);
			}
		}
	}

	// write existing buffer to file
	if (fwrite(write_buffer, 1, write_len, f) != write_len) {
		Log("Failed to write data %s: %s\n", temporary_file_name, strerror(errno));
		fclose(f);
		remove(temporary_file_name.c_str());
		Terminal::Beep();
		return false;
	}
	fclose(f);

	/* call editor */
	if (!EditFileWithEditor(temporary_file_name, editor)) {
		Terminal::Beep();
		return false;
	}

	// read the file contents again
	f = fopen(temporary_file_name.c_str(), "rb");
	if (!f) {
		Log("Failed to open file%s: %s\n", temporary_file_name, strerror(errno));
		remove(temporary_file_name.c_str());
		Terminal::Beep();
		return false;
	}

	/* read file back into buffer */
	string new_buffer;
	char line[1024];
	while (fgets(line, sizeof(line), f)) {
		// strip the existing newline from the line obtained from fgets
		// the reason for that is that we need the line endings to be "\r\n" for rendering purposes
		idx_t line_len = strlen(line);
		idx_t orig_len = line_len;
		while (line_len > 0 && (line[line_len - 1] == '\r' || line[line_len - 1] == '\n')) {
			line_len--;
		}
		new_buffer.append(line, line_len);
		if (orig_len != line_len) {
			// we stripped a newline - add a new newline (but this time always \r\n)
			new_buffer += "\r\n";
		}
	}
	if (ferror(f)) {
		Log("Failed while reading back buffer %s: %s\n", temporary_file_name, strerror(errno));
		Terminal::Beep();
	}
	fclose(f);

	/* remove temp file */
	if (remove(temporary_file_name.c_str()) == -1) {
		Log("Failed to remove file \"%s\": %s\n", temporary_file_name, strerror(errno));
		Terminal::Beep();
		return false;
	}

	// copy back into buffer
	memcpy(buf, new_buffer.c_str(), new_buffer.size());
	len = new_buffer.size();
	pos = len;
	RefreshLine();

	return true;
}
#endif

} // namespace duckdb
