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
#include "terminal.hpp"
#include "utf8proc_wrapper.hpp"
#include <unordered_set>
#include <vector>
#include "duckdb_shell_wrapper.h"
#include <string>
#include "sqlite3.h"
#ifndef DISABLE_HIGHLIGHTING
#include <sstream>
#include "duckdb/parser/parser.hpp"
#endif
#ifdef __MVS__
#include <strings.h>
#include <sys/time.h>
#endif

namespace duckdb {

#if defined(_WIN32) || defined(__WIN32__) || defined(WIN32)
	// disable highlighting on windows (for now?)
#define DISABLE_HIGHLIGHT
#endif

static linenoiseCompletionCallback *completionCallback = NULL;
static linenoiseHintsCallback *hintsCallback = NULL;
static linenoiseFreeHintsCallback *freeHintsCallback = NULL;

#ifndef DISABLE_HIGHLIGHT

static int enableHighlighting = 1;
struct Color {
	const char *color_name;
	const char *highlight;
};
static Color terminal_colors[] = {{"red",           "\033[31m"},
								  {"green",         "\033[32m"},
								  {"yellow",        "\033[33m"},
								  {"blue",          "\033[34m"},
								  {"magenta",       "\033[35m"},
								  {"cyan",          "\033[36m"},
								  {"white",         "\033[37m"},
								  {"brightblack",   "\033[90m"},
								  {"brightred",     "\033[91m"},
								  {"brightgreen",   "\033[92m"},
								  {"brightyellow",  "\033[93m"},
								  {"brightblue",    "\033[94m"},
								  {"brightmagenta", "\033[95m"},
								  {"brightcyan",    "\033[96m"},
								  {"brightwhite",   "\033[97m"},
								  {nullptr,         nullptr}};
static std::string bold = "\033[1m";
static std::string underline = "\033[4m";
static std::string keyword = "\033[32m";
static std::string constant = "\033[33m";
static std::string reset = "\033[00m";
#endif

static const char *continuationPrompt = "> ";
static const char *continuationSelectedPrompt = "> ";

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
	char *new_entry = (char *) realloc(ab->b, ab->len + len);

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
void Linenoise::RefreshShowHints(struct abuf *ab, int plen) {
	char seq[64];
	if (hintsCallback && plen + len < size_t(ws.ws_col)) {
		int color = -1, bold = 0;
		char *hint = hintsCallback(buf, &color, &bold);
		if (hint) {
			int hintlen = strlen(hint);
			int hintmaxlen = ws.ws_col - (plen + len);
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

int Linenoise::GetPromptWidth() {
	return ComputeRenderWidth(prompt, strlen(prompt));
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

int Linenoise::ParseOption(const char **azArg, int nArg, const char **out_error) {
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

void Linenoise::SetPrompt(const char *continuation, const char *continuationSelected) {
	continuationPrompt = continuation;
	continuationSelectedPrompt = continuationSelected;
}

#ifndef DISABLE_HIGHLIGHT

tokenType convertToken(duckdb::SimplifiedTokenType token_type) {
	switch (token_type) {
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
			return tokenType::TOKEN_IDENTIFIER;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
			return tokenType::TOKEN_NUMERIC_CONSTANT;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
			return tokenType::TOKEN_STRING_CONSTANT;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR:
			return tokenType::TOKEN_OPERATOR;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
			return tokenType::TOKEN_KEYWORD;
		case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
			return tokenType::TOKEN_COMMENT;
		default:
			throw duckdb::InternalException("Unrecognized token type");
	}
}

std::vector<highlightToken> tokenize(char *buf, size_t len, searchMatch *match = nullptr) {
	std::string sql(buf, len);
	auto parseTokens = duckdb::Parser::Tokenize(sql);
	std::vector<highlightToken> tokens;

	for (auto &token: parseTokens) {
		highlightToken new_token;
		new_token.type = convertToken(token.type);
		new_token.start = token.start;
		tokens.push_back(new_token);
	}

	if (!tokens.empty() && tokens[0].start > 0) {
		highlightToken new_token;
		new_token.type = tokenType::TOKEN_IDENTIFIER;
		new_token.start = 0;
		tokens.insert(tokens.begin(), new_token);
	}
	if (tokens.empty() && sql.size() > 0) {
		highlightToken new_token;
		new_token.type = tokenType::TOKEN_IDENTIFIER;
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
				auto end_type = tokens[i].type;
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
	return tokens;
}

std::string highlightText(char *buf, size_t len, size_t start_pos, size_t end_pos,
						  const std::vector<highlightToken> &tokens) {
	std::stringstream ss;
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
			case tokenType::TOKEN_KEYWORD:
			case tokenType::TOKEN_CONTINUATION_SELECTED:
				ss << keyword << text << reset;
				break;
			case tokenType::TOKEN_NUMERIC_CONSTANT:
			case tokenType::TOKEN_STRING_CONSTANT:
			case tokenType::TOKEN_CONTINUATION:
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
			auto tokens = tokenize(buf, len, match);
			highlight_buffer = highlightText(buf, len, start_pos, cpos, tokens);
			buf = (char *) highlight_buffer.c_str();
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
void Linenoise::RefreshSingleLine() {
	char seq[64];
	size_t plen = GetPromptWidth();
	int fd = ofd;
	char *buf = buf;
	size_t len = len;
	struct abuf ab;
	size_t render_pos = 0;
	std::string highlight_buffer;

	renderText(render_pos, buf, len, pos, ws.ws_col, plen, highlight_buffer, enableHighlighting);

	abInit(&ab);
	/* Cursor to left edge */
	snprintf(seq, 64, "\r");
	abAppend(&ab, seq, strlen(seq));
	/* Write the prompt and the current buffer content */
	abAppend(&ab, prompt, strlen(prompt));
	abAppend(&ab, buf, len);
	/* Show hits if any. */
	RefreshShowHints(&ab, plen);
	/* Erase to right */
	snprintf(seq, 64, "\x1b[0K");
	abAppend(&ab, seq, strlen(seq));
	/* Move cursor to original position. */
	snprintf(seq, 64, "\r\x1b[%dC", (int) (render_pos + plen));
	abAppend(&ab, seq, strlen(seq));
	if (write(fd, ab.b, ab.len) == -1) {
	} /* Can't recover from write error. */
	abFree(&ab);
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
			char *search_buf = (char *) search_text.c_str();
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
	auto oldHighlighting = enableHighlighting;
	Linenoise clone = *this;
	prompt = (char *) search_prompt.c_str();
	plen = search_prompt.size();
	if (no_matches || search_buf.empty()) {
		// if there are no matches render the no_matches_text
		buf = (char *) no_matches_text.c_str();
		len = no_matches_text.size();
		pos = 0;
		// don't highlight the "no_matches" text
		enableHighlighting = false;
	} else {
		// if there are matches render the current history item
		auto search_match = search_matches[search_index];
		auto history_index = search_match.history_index;
		auto cursor_position = search_match.match_end;
		buf = (char *) History::GetEntry(history_index);
		len = strlen(buf);
		pos = cursor_position;
	}
	RefreshLine();

	enableHighlighting = oldHighlighting;
	buf = clone.buf;
	len = clone.len;
	pos = clone.pos;
	prompt = clone.prompt;
	plen = clone.plen;
}

bool isNewline(char c) {
	return c == '\r' || c == '\n';
}

void Linenoise::NextPosition(const char *buf, size_t len, size_t &cpos, int &rows, int &cols, int plen) {
	if (isNewline(buf[cpos])) {
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
		char_render_width = (int) duckdb::Utf8Proc::RenderWidth(buf, len, cpos);
		cpos = duckdb::Utf8Proc::NextGraphemeCluster(buf, len, cpos);
	}
	if (cols + char_render_width > ws.ws_col) {
		// exceeded l->cols, move to next row
		rows++;
		cols = char_render_width;
	}
	cols += char_render_width;
}

void Linenoise::PositionToColAndRow(size_t target_pos, int &out_row, int &out_col, int &rows, int &cols) {
	int plen = GetPromptWidth();
	out_row = -1;
	out_col = 0;
	rows = 1;
	cols = plen;
	size_t cpos = 0;
	while (cpos < len) {
		if (cols >= ws.ws_col && !isNewline(buf[cpos])) {
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

size_t Linenoise::ColAndRowToPosition(int target_row, int target_col) {
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

std::string Linenoise::AddContinuationMarkers(const char *buf, size_t len, int plen,
										  int cursor_row, std::vector<highlightToken> &tokens) {
	std::string result;
	int rows = 1;
	int cols = plen;
	size_t cpos = 0;
	size_t prev_pos = 0;
	size_t extra_bytes = 0;    // extra bytes introduced
	size_t token_position = 0; // token position
	std::vector<highlightToken> new_tokens;
	new_tokens.reserve(tokens.size());
	while (cpos < len) {
		bool is_newline = isNewline(buf[cpos]);
		NextPosition(buf, len, cpos, rows, cols, plen);
		for (; prev_pos < cpos; prev_pos++) {
			result += buf[prev_pos];
		}
		if (is_newline) {
			bool is_cursor_row = rows == cursor_row;
			const char *prompt = is_cursor_row ? continuationSelectedPrompt : continuationPrompt;
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
	struct abuf ab;
	std::string highlight_buffer;
	auto buf = this->buf;
	auto len = this->len;
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
		buf += start;
		len = end - start;
		lndebug("truncate to rows %d - %d (render bytes %d to %d)", y_scroll, y_scroll + ws.ws_row, start,
				end);
		rows = ws.ws_row;
	} else {
		y_scroll = 0;
	}

	/* Update maxrows if needed. */
	if (rows > (int) maxrows) {
		maxrows = rows;
	}

	std::vector<highlightToken> tokens;
#ifndef DISABLE_HIGHLIGHT
	auto match = search_index < search_matches.size() ? &search_matches[search_index] : nullptr;
	tokens = tokenize(buf, len, match);
#endif
	if (rows > 1) {
		// add continuation markers
		highlight_buffer =
				AddContinuationMarkers(buf, len, plen, y_scroll > 0 ? new_cursor_row + 1 : new_cursor_row,
									   tokens);
		buf = (char *) highlight_buffer.c_str();
		len = highlight_buffer.size();
	}
#ifndef DISABLE_HIGHLIGHT
	if (duckdb::Utf8Proc::IsValid(buf, len)) {
		if (enableHighlighting) {
			highlight_buffer = highlightText(buf, len, 0, len, tokens);
			buf = (char *) highlight_buffer.c_str();
			len = highlight_buffer.size();
		}
	}
#endif

	/* First step: clear all the lines used before. To do so start by
	 * going to the last row. */
	abInit(&ab);
	if (old_rows - old_cursor_rows > 0) {
		lndebug("go down %d", old_rows - old_cursor_rows);
		snprintf(seq, 64, "\x1b[%dB", old_rows - int(old_cursor_rows));
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
	if (y_scroll == 0) {
		abAppend(&ab, prompt, strlen(prompt));
	}
	abAppend(&ab, buf, len);

	/* Show hints if any. */
	RefreshShowHints(&ab, plen);

	/* If we are at the very end of the screen with our prompt, we need to
	 * emit a newline and move the prompt to the first column. */
	lndebug("pos > 0 %d", pos > 0 ? 1 : 0);
	lndebug("pos == len %d", pos == len ? 1 : 0);
	lndebug("new_cursor_x == cols %d", new_cursor_x == ws.ws_col ? 1 : 0);
	if (pos > 0 && pos == len && new_cursor_x == ws.ws_col) {
		lndebug("<newline>", 0);
		abAppend(&ab, "\n", 1);
		snprintf(seq, 64, "\r");
		abAppend(&ab, seq, strlen(seq));
		rows++;
		new_cursor_row++;
		new_cursor_x = 0;
		if (rows > (int) maxrows) {
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
		abAppend(&ab, seq, strlen(seq));
	}

	/* Set column. */
	col = new_cursor_x;
	lndebug("set col %d", 1 + col);
	if (col)
		snprintf(seq, 64, "\r\x1b[%dC", col);
	else
		snprintf(seq, 64, "\r");
	abAppend(&ab, seq, strlen(seq));

	lndebug("\n", 0);
	old_cursor_rows = new_cursor_row;

	if (write(fd, ab.b, ab.len) == -1) {
	} /* Can't recover from write error. */
	abFree(&ab);
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

size_t Linenoise::PrevChar() {
	return duckdb::Utf8Proc::PreviousGraphemeCluster(buf, len, pos);
}

size_t Linenoise::NextChar() {
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
void Linenoise::EditMoveWordLeft() {
	if (pos == 0) {
		return;
	}
	do {
		pos = PrevChar();
	} while (pos > 0 && !characterIsWordBoundary(buf[pos]));
	RefreshLine();
}

/* Move cursor on the right. */
void Linenoise::EditMoveWordRight() {
	if (pos == len) {
		return;
	}
	do {
		pos = NextChar();
	} while (pos != len && !characterIsWordBoundary(buf[pos]));
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
#define LINENOISE_HISTORY_NEXT 0
#define LINENOISE_HISTORY_PREV 1

void Linenoise::EditHistoryNext(int dir) {
	auto history_len = History::GetLength();
	if (history_len > 1) {
		/* Update the current history entry before to
		 * overwrite it with the next one. */
		History::Overwrite(history_len - 1 - history_index, buf);
		/* Show the new entry */
		history_index += (dir == LINENOISE_HISTORY_PREV) ? 1 : -1;
		if (history_index < 0) {
			history_index = 0;
			return;
		} else if (history_index >= history_len) {
			history_index = history_len - 1;
			return;
		}
		strncpy(buf, History::GetEntry(history_len - 1 - history_index), buflen);
		buf[buflen - 1] = '\0';
		len = pos = strlen(buf);
		if (Terminal::IsMultiline() && dir == LINENOISE_HISTORY_NEXT) {
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

static int allWhitespace(const char *z) {
	for (; *z; z++) {
		if (isspace((unsigned char) z[0]))
			continue;
		if (*z == '/' && z[1] == '*') {
			z += 2;
			while (*z && (*z != '*' || z[1] != '/')) {
				z++;
			}
			if (*z == 0)
				return 0;
			z++;
			continue;
		}
		if (*z == '-' && z[1] == '-') {
			z += 2;
			while (*z && *z != '\n') {
				z++;
			}
			if (*z == 0)
				return 1;
			continue;
		}
		return 0;
	}
	return 1;
}

Linenoise::Linenoise(int stdin_fd, int stdout_fd, char *buf, size_t buflen, const char *prompt) :
	ifd(stdin_fd), ofd(stdout_fd), buf(buf), buflen(buflen), prompt(prompt), plen(strlen(prompt)) {
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

	if (write(ofd, prompt, plen) == -1)
		return -1;
	while (1) {
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
		if (c == 9 && completionCallback != NULL) {
			if (has_more_data) {
				// if there is more data, this tab character was added as part of copy-pasting data
				continue;
			}
			c = CompleteLine();
			/* Return on errors */
			if (c < 0)
				return len;
			/* Read next character when 0 */
			if (c == 0)
				continue;
		}

		lndebug("%d\n", (int) c);
		switch (c) {
			case 10:
			case ENTER: /* enter */
				if (Terminal::IsMultiline() && len > 0) {
					// check if this forms a complete SQL statement or not
					buf[len] = '\0';
					if (buf[0] != '.' && !allWhitespace(buf) && !sqlite3_complete(buf)) {
						// not a complete SQL statement yet! continuation
						// insert "\r\n"
						if (EditInsertMulti("\r\n")) {
							return -1;
						}
						break;
					}
				}
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
				return (int) len;
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
				return (int) len;
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
				EditHistoryNext(LINENOISE_HISTORY_PREV);
				break;
			case CTRL_N: /* ctrl-n */
				EditHistoryNext(LINENOISE_HISTORY_NEXT);
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
								EditHistoryNext(LINENOISE_HISTORY_PREV);
								break;
							case 'B': /* Down */
								if (EditMoveRowDown()) {
									break;
								}
								EditHistoryNext(LINENOISE_HISTORY_NEXT);
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

}
