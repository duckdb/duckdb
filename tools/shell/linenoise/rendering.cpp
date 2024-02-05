#include "linenoise.hpp"
#include "highlighting.hpp"
#include "history.hpp"
#include "utf8proc_wrapper.hpp"
#include <unistd.h>

namespace duckdb {
static const char *continuationPrompt = "> ";
static const char *continuationSelectedPrompt = "> ";

/* =========================== Line editing ================================= */

/* We define a very simple "append buffer" structure, that is an heap
 * allocated string where we can append to. This is useful in order to
 * write all the escape sequences in a buffer and flush them to the standard
 * output in a single call, to avoid flickering effects. */
struct AppendBuffer {
	void Append(const char *s, idx_t len) {
		buffer.append(s, len);
	}
	void Write(int fd) {
		if (write(fd, buffer.c_str(), buffer.size()) == -1) {
			/* Can't recover from write error. */
			lndebug("%s", "Failed to write buffer\n");
		}
	}

private:
	std::string buffer;
};

void Linenoise::SetPrompt(const char *continuation, const char *continuationSelected) {
	continuationPrompt = continuation;
	continuationSelectedPrompt = continuationSelected;
}

/* Helper of refreshSingleLine() and refreshMultiLine() to show hints
 * to the right of the prompt. */
void Linenoise::RefreshShowHints(AppendBuffer &append_buffer, int plen) const {
	char seq[64];
	auto hints_callback = Linenoise::HintsCallback();
	if (hints_callback && plen + len < size_t(ws.ws_col)) {
		int color = -1, bold = 0;
		char *hint = hints_callback(buf, &color, &bold);
		if (hint) {
			int hintlen = strlen(hint);
			int hintmaxlen = ws.ws_col - (plen + len);
			if (hintlen > hintmaxlen) {
				hintlen = hintmaxlen;
			}
			if (bold == 1 && color == -1)
				color = 37;
			if (color != -1 || bold != 0) {
				snprintf(seq, 64, "\033[%d;%d;49m", bold, color);
			} else {
				seq[0] = '\0';
			}
			append_buffer.Append(seq, strlen(seq));
			append_buffer.Append(hint, hintlen);
			if (color != -1 || bold != 0) {
				append_buffer.Append("\033[0m", 4);
			}
			/* Call the function to free the hint returned. */
			auto free_hints_callback = Linenoise::FreeHintsCallback();
			if (free_hints_callback) {
				free_hints_callback(hint);
			}
		}
	}
}

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
		if (highlight) {
			auto tokens = Highlighting::Tokenize(buf, len, match);
			highlight_buffer = Highlighting::HighlightText(buf, len, start_pos, cpos, tokens);
			buf = (char *)highlight_buffer.c_str();
			len = highlight_buffer.size();
		} else {
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
void Linenoise::RefreshSingleLine() const {
	char seq[64];
	size_t plen = GetPromptWidth();
	int fd = ofd;
	char *render_buf = buf;
	size_t render_len = len;
	size_t render_pos = 0;
	std::string highlight_buffer;

	renderText(render_pos, render_buf, render_len, pos, ws.ws_col, plen, highlight_buffer, Highlighting::IsEnabled());

	AppendBuffer append_buffer;
	/* Cursor to left edge */
	snprintf(seq, 64, "\r");
	append_buffer.Append(seq, strlen(seq));
	/* Write the prompt and the current buffer content */
	append_buffer.Append(prompt, strlen(prompt));
	append_buffer.Append(render_buf, render_len);
	/* Show hits if any. */
	RefreshShowHints(append_buffer, plen);
	/* Erase to right */
	snprintf(seq, 64, "\x1b[0K");
	append_buffer.Append(seq, strlen(seq));
	/* Move cursor to original position. */
	snprintf(seq, 64, "\r\x1b[%dC", (int)(render_pos + plen));
	append_buffer.Append(seq, strlen(seq));
	append_buffer.Write(fd);
}

void Linenoise::RefreshSearch() {
	std::string search_prompt;
	static const size_t SEARCH_PROMPT_RENDER_SIZE = 28;
	std::string no_matches_text = "(no matches)";
	bool no_matches = search_index >= search_matches.size();
	if (search_buf.empty()) {
		search_prompt = "search" + std::string(SEARCH_PROMPT_RENDER_SIZE - 8, ' ') + "> ";
		no_matches_text = "(type to search)";
	} else {
		std::string search_text;
		std::string matches_text;
		search_text += search_buf;
		if (!no_matches) {
			matches_text += std::to_string(search_index + 1);
			matches_text += "/" + std::to_string(search_matches.size());
		}
		size_t search_text_length = ComputeRenderWidth(search_text.c_str(), search_text.size());
		size_t matches_text_length = ComputeRenderWidth(matches_text.c_str(), matches_text.size());
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
	auto oldHighlighting = Highlighting::IsEnabled();
	Linenoise clone = *this;
	prompt = search_prompt.c_str();
	plen = search_prompt.size();
	if (no_matches || search_buf.empty()) {
		// if there are no matches render the no_matches_text
		buf = (char *)no_matches_text.c_str();
		len = no_matches_text.size();
		pos = 0;
		// don't highlight the "no_matches" text
		Highlighting::Disable();
	} else {
		// if there are matches render the current history item
		auto search_match = search_matches[search_index];
		auto history_index = search_match.history_index;
		auto cursor_position = search_match.match_end;
		buf = (char *)History::GetEntry(history_index);
		len = strlen(buf);
		pos = cursor_position;
	}
	RefreshLine();

	if (oldHighlighting) {
		Highlighting::Enable();
	}
	buf = clone.buf;
	len = clone.len;
	pos = clone.pos;
	prompt = clone.prompt;
	plen = clone.plen;
}

string Linenoise::AddContinuationMarkers(const char *buf, size_t len, int plen, int cursor_row,
                                         vector<highlightToken> &tokens) const {
	std::string result;
	int rows = 1;
	int cols = plen;
	size_t cpos = 0;
	size_t prev_pos = 0;
	size_t extra_bytes = 0;    // extra bytes introduced
	size_t token_position = 0; // token position
	vector<highlightToken> new_tokens;
	new_tokens.reserve(tokens.size());
	while (cpos < len) {
		bool is_newline = IsNewline(buf[cpos]);
		NextPosition(buf, len, cpos, rows, cols, plen);
		for (; prev_pos < cpos; prev_pos++) {
			result += buf[prev_pos];
		}
		if (is_newline) {
			bool is_cursor_row = rows == cursor_row;
			const char *prompt = is_cursor_row ? continuationSelectedPrompt : continuationPrompt;
			if (!continuation_markers) {
				prompt = "";
			}
			size_t continuationLen = strlen(prompt);
			size_t continuationRender = ComputeRenderWidth(prompt, continuationLen);
			// pad with spaces prior to prompt
			for (int i = int(continuationRender); i < plen; i++) {
				result += " ";
			}
			result += prompt;
			size_t continuationBytes = plen - continuationRender + continuationLen;
			if (token_position < tokens.size()) {
				for (; token_position < tokens.size(); token_position++) {
					if (tokens[token_position].start >= cpos) {
						// not there yet
						break;
					}
					tokens[token_position].start += extra_bytes;
					new_tokens.push_back(tokens[token_position]);
				}
				tokenType prev_type = tokenType::TOKEN_IDENTIFIER;
				if (token_position > 0 && token_position < tokens.size() + 1) {
					prev_type = tokens[token_position - 1].type;
				}
				highlightToken token;
				token.start = cpos + extra_bytes;
				token.type = is_cursor_row ? tokenType::TOKEN_CONTINUATION_SELECTED : tokenType::TOKEN_CONTINUATION;
				token.search_match = false;
				new_tokens.push_back(token);

				token.start = cpos + extra_bytes + continuationBytes;
				token.type = prev_type;
				token.search_match = false;
				new_tokens.push_back(token);
			}
			extra_bytes += continuationBytes;
		}
	}
	for (; prev_pos < cpos; prev_pos++) {
		result += buf[prev_pos];
	}
	for (; token_position < tokens.size(); token_position++) {
		tokens[token_position].start += extra_bytes;
		new_tokens.push_back(tokens[token_position]);
	}
	tokens = std::move(new_tokens);
	return result;
}

/* Multi line low level line refresh.
 *
 * Rewrite the currently edited line accordingly to the buffer content,
 * cursor position, and number of columns of the terminal. */
void Linenoise::RefreshMultiLine() {
	if (!render) {
		return;
	}
	char seq[64];
	int plen = GetPromptWidth();
	// utf8 in prompt, get render width
	int rows, cols;
	int new_cursor_row, new_cursor_x;
	PositionToColAndRow(pos, new_cursor_row, new_cursor_x, rows, cols);
	int col; /* colum position, zero-based. */
	int old_rows = maxrows ? maxrows : 1;
	int fd = ofd, j;
	std::string highlight_buffer;
	auto render_buf = this->buf;
	auto render_len = this->len;
	if (clear_screen) {
		old_cursor_rows = 0;
		old_rows = 0;
		clear_screen = false;
	}
	if (rows > ws.ws_row) {
		// the text does not fit in the terminal (too many rows)
		// enable scrolling mode
		// check if, given the current y_scroll, the cursor is visible
		// display range is [y_scroll, y_scroll + ws.ws_row]
		if (new_cursor_row < int(y_scroll) + 1) {
			y_scroll = new_cursor_row - 1;
		} else if (new_cursor_row > int(y_scroll) + int(ws.ws_row)) {
			y_scroll = new_cursor_row - ws.ws_row;
		}
		// display only characters up to the current scroll position
		int start, end;
		if (y_scroll == 0) {
			start = 0;
		} else {
			start = ColAndRowToPosition(y_scroll, 0);
		}
		if (int(y_scroll) + int(ws.ws_row) >= rows) {
			end = len;
		} else {
			end = ColAndRowToPosition(y_scroll + ws.ws_row, 99999);
		}
		new_cursor_row -= y_scroll;
		render_buf += start;
		render_len = end - start;
		lndebug("truncate to rows %d - %d (render bytes %d to %d)", y_scroll, y_scroll + ws.ws_row, start, end);
		rows = ws.ws_row;
	} else {
		y_scroll = 0;
	}

	/* Update maxrows if needed. */
	if (rows > (int)maxrows) {
		maxrows = rows;
	}

	vector<highlightToken> tokens;
	if (Highlighting::IsEnabled()) {
		auto match = search_index < search_matches.size() ? &search_matches[search_index] : nullptr;
		tokens = Highlighting::Tokenize(render_buf, render_len, match);
	}
	if (rows > 1) {
		// add continuation markers
		highlight_buffer = AddContinuationMarkers(render_buf, render_len, plen,
		                                          y_scroll > 0 ? new_cursor_row + 1 : new_cursor_row, tokens);
		render_buf = (char *)highlight_buffer.c_str();
		render_len = highlight_buffer.size();
	}
	if (duckdb::Utf8Proc::IsValid(render_buf, render_len)) {
		if (Highlighting::IsEnabled()) {
			highlight_buffer = Highlighting::HighlightText(render_buf, render_len, 0, render_len, tokens);
			render_buf = (char *)highlight_buffer.c_str();
			render_len = highlight_buffer.size();
		}
	}

	/* First step: clear all the lines used before. To do so start by
	 * going to the last row. */
	AppendBuffer append_buffer;
	if (old_rows - old_cursor_rows > 0) {
		lndebug("go down %d", old_rows - old_cursor_rows);
		snprintf(seq, 64, "\x1b[%dB", old_rows - int(old_cursor_rows));
		append_buffer.Append(seq, strlen(seq));
	}

	/* Now for every row clear it, go up. */
	for (j = 0; j < old_rows - 1; j++) {
		lndebug("clear+up", 0);
		snprintf(seq, 64, "\r\x1b[0K\x1b[1A");
		append_buffer.Append(seq, strlen(seq));
	}

	/* Clean the top line. */
	lndebug("clear", 0);
	snprintf(seq, 64, "\r\x1b[0K");
	append_buffer.Append(seq, strlen(seq));

	/* Write the prompt and the current buffer content */
	if (y_scroll == 0) {
		append_buffer.Append(prompt, strlen(prompt));
	}
	append_buffer.Append(render_buf, render_len);

	/* Show hints if any. */
	RefreshShowHints(append_buffer, plen);

	/* If we are at the very end of the screen with our prompt, we need to
	 * emit a newline and move the prompt to the first column. */
	lndebug("pos > 0 %d", pos > 0 ? 1 : 0);
	lndebug("pos == len %d", pos == len ? 1 : 0);
	lndebug("new_cursor_x == cols %d", new_cursor_x == ws.ws_col ? 1 : 0);
	if (pos > 0 && pos == len && new_cursor_x == ws.ws_col) {
		lndebug("<newline>", 0);
		append_buffer.Append("\n", 1);
		snprintf(seq, 64, "\r");
		append_buffer.Append(seq, strlen(seq));
		rows++;
		new_cursor_row++;
		new_cursor_x = 0;
		if (rows > (int)maxrows) {
			maxrows = rows;
		}
	}
	lndebug("render %d rows (old rows %d)", rows, old_rows);

	/* Move cursor to right position. */
	lndebug("new_cursor_row %d", new_cursor_row);
	lndebug("new_cursor_x %d", new_cursor_x);
	lndebug("len %d", len);
	lndebug("old_cursor_rows %d", old_cursor_rows);
	lndebug("pos %d", pos);
	lndebug("max cols %d", ws.ws_col);

	/* Go up till we reach the expected positon. */
	if (rows - new_cursor_row > 0) {
		lndebug("go-up %d", rows - new_cursor_row);
		snprintf(seq, 64, "\x1b[%dA", rows - new_cursor_row);
		append_buffer.Append(seq, strlen(seq));
	}

	/* Set column. */
	col = new_cursor_x;
	lndebug("set col %d", 1 + col);
	if (col) {
		snprintf(seq, 64, "\r\x1b[%dC", col);
	} else {
		snprintf(seq, 64, "\r");
	}
	append_buffer.Append(seq, strlen(seq));

	lndebug("\n", 0);
	old_cursor_rows = new_cursor_row;
	append_buffer.Write(fd);
}

/* Calls the two low level functions refreshSingleLine() or
 * refreshMultiLine() according to the selected mode. */
void Linenoise::RefreshLine() {
	if (Terminal::IsMultiline()) {
		RefreshMultiLine();
	} else {
		RefreshSingleLine();
	}
}

} // namespace duckdb
