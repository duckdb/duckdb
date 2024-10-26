#include "linenoise.hpp"
#include "highlighting.hpp"
#include "history.hpp"
#include "utf8proc_wrapper.hpp"
#include <unistd.h>

namespace duckdb {
static const char *continuationPrompt = "> ";
static const char *continuationSelectedPrompt = "> ";
static bool enableCompletionRendering = false;
static bool enableErrorRendering = true;

void Linenoise::EnableCompletionRendering() {
	enableCompletionRendering = true;
}

void Linenoise::DisableCompletionRendering() {
	enableCompletionRendering = false;
}

void Linenoise::EnableErrorRendering() {
	enableErrorRendering = true;
}

void Linenoise::DisableErrorRendering() {
	enableErrorRendering = false;
}

/* =========================== Line editing ================================= */

/* We define a very simple "append buffer" structure, that is an heap
 * allocated string where we can append to. This is useful in order to
 * write all the escape sequences in a buffer and flush them to the standard
 * output in a single call, to avoid flickering effects. */
struct AppendBuffer {
	void Append(const char *s, idx_t len) {
		buffer.append(s, len);
	}
	void Append(const char *s) {
		buffer.append(s);
	}

	void Write(int fd) {
		if (write(fd, buffer.c_str(), buffer.size()) == -1) {
			/* Can't recover from write error. */
			Linenoise::Log("%s", "Failed to write buffer\n");
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
				append_buffer.Append("\033[0m");
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
			bool is_dot_command = buf[0] == '.';

			auto tokens = Highlighting::Tokenize(buf, len, is_dot_command, match);
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
	append_buffer.Append("\r");
	/* Write the prompt and the current buffer content */
	append_buffer.Append(prompt);
	append_buffer.Append(render_buf, render_len);
	/* Show hits if any. */
	RefreshShowHints(append_buffer, plen);
	/* Erase to right */
	append_buffer.Append("\x1b[0K");
	/* Move cursor to original position. */
	snprintf(seq, 64, "\r\x1b[%dC", (int)(render_pos + plen));
	append_buffer.Append(seq);
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

// insert a token of length 1 of the specified type
void InsertToken(tokenType insert_type, idx_t insert_pos, vector<highlightToken> &tokens) {
	vector<highlightToken> new_tokens;
	new_tokens.reserve(tokens.size() + 1);
	idx_t i;
	bool found = false;
	for (i = 0; i < tokens.size(); i++) {
		// find the exact position where we need to insert the token
		if (tokens[i].start == insert_pos) {
			// this token is exactly at this render position

			// insert highlighting for the bracket
			highlightToken token;
			token.start = insert_pos;
			token.type = insert_type;
			token.search_match = false;
			new_tokens.push_back(token);

			// now we need to insert the other token ONLY if the other token is not immediately following this one
			if (i + 1 >= tokens.size() || tokens[i + 1].start > insert_pos + 1) {
				token.start = insert_pos + 1;
				token.type = tokens[i].type;
				token.search_match = false;
				new_tokens.push_back(token);
			}
			i++;
			found = true;
			break;
		} else if (tokens[i].start > insert_pos) {
			// the next token is AFTER the render position
			// insert highlighting for the bracket
			highlightToken token;
			token.start = insert_pos;
			token.type = insert_type;
			token.search_match = false;
			new_tokens.push_back(token);

			// now just insert the next token
			new_tokens.push_back(tokens[i]);
			i++;
			found = true;
			break;
		} else {
			// insert the token
			new_tokens.push_back(tokens[i]);
		}
	}
	// copy over the remaining tokens
	for (; i < tokens.size(); i++) {
		new_tokens.push_back(tokens[i]);
	}
	if (!found) {
		// token was not added - add it to the end
		highlightToken token;
		token.start = insert_pos;
		token.type = insert_type;
		token.search_match = false;
		new_tokens.push_back(token);
	}
	tokens = std::move(new_tokens);
}

enum class ScanState { STANDARD, IN_SINGLE_QUOTE, IN_DOUBLE_QUOTE, IN_COMMENT, DOLLAR_QUOTED_STRING };

static void OpenBracket(vector<idx_t> &brackets, vector<idx_t> &cursor_brackets, idx_t pos, idx_t i) {
	// check if the cursor is at this position
	if (pos == i) {
		// cursor is exactly on this position - always highlight this bracket
		if (!cursor_brackets.empty()) {
			cursor_brackets.clear();
		}
		cursor_brackets.push_back(i);
	}
	if (cursor_brackets.empty() && ((i + 1) == pos || (pos + 1) == i)) {
		// cursor is either BEFORE or AFTER this bracket and we don't have any highlighted bracket yet
		// highlight this bracket
		cursor_brackets.push_back(i);
	}
	brackets.push_back(i);
}

static void CloseBracket(vector<idx_t> &brackets, vector<idx_t> &cursor_brackets, idx_t pos, idx_t i,
                         vector<idx_t> &errors) {
	if (pos == i) {
		// cursor is on this closing bracket
		// clear any selected brackets - we always select this one
		cursor_brackets.clear();
	}
	if (brackets.empty()) {
		// closing bracket without matching opening bracket
		errors.push_back(i);
	} else {
		if (cursor_brackets.size() == 1) {
			if (cursor_brackets.back() == brackets.back()) {
				// this closing bracket matches the highlighted opening cursor bracket - highlight both
				cursor_brackets.push_back(i);
			}
		} else if (cursor_brackets.empty() && (pos == i || (i + 1) == pos || (pos + 1) == i)) {
			// no cursor bracket selected yet and cursor is BEFORE or AFTER this bracket
			// add this bracket
			cursor_brackets.push_back(i);
			cursor_brackets.push_back(brackets.back());
		}
		brackets.pop_back();
	}
}

static void HandleBracketErrors(const vector<idx_t> &brackets, vector<idx_t> &errors) {
	if (brackets.empty()) {
		return;
	}
	// if there are unclosed brackets remaining not all brackets were closed
	for (auto &bracket : brackets) {
		errors.push_back(bracket);
	}
}

void Linenoise::AddErrorHighlighting(idx_t render_start, idx_t render_end, vector<highlightToken> &tokens) const {
	static constexpr const idx_t MAX_ERROR_LENGTH = 2000;
	if (!enableErrorRendering) {
		return;
	}
	if (len >= MAX_ERROR_LENGTH) {
		return;
	}
	// do a pass over the buffer to collect errors:
	// * brackets without matching closing/opening bracket
	// * single quotes without matching closing single quote
	// * double quote without matching double quote
	ScanState state = ScanState::STANDARD;
	vector<idx_t> brackets;        // ()
	vector<idx_t> square_brackets; // []
	vector<idx_t> curly_brackets;  // {}
	vector<idx_t> errors;
	vector<idx_t> cursor_brackets;
	vector<idx_t> comment_start;
	vector<idx_t> comment_end;
	string dollar_quote_marker;
	idx_t quote_pos = 0;
	for (idx_t i = 0; i < len; i++) {
		auto c = buf[i];
		switch (state) {
		case ScanState::STANDARD:
			switch (c) {
			case '-':
				if (i + 1 < len && buf[i + 1] == '-') {
					// -- puts us in a comment
					comment_start.push_back(i);
					i++;
					state = ScanState::IN_COMMENT;
					break;
				}
				break;
			case '\'':
				state = ScanState::IN_SINGLE_QUOTE;
				quote_pos = i;
				break;
			case '\"':
				state = ScanState::IN_DOUBLE_QUOTE;
				quote_pos = i;
				break;
			case '(':
				OpenBracket(brackets, cursor_brackets, pos, i);
				break;
			case '[':
				OpenBracket(square_brackets, cursor_brackets, pos, i);
				break;
			case '{':
				OpenBracket(curly_brackets, cursor_brackets, pos, i);
				break;
			case ')':
				CloseBracket(brackets, cursor_brackets, pos, i, errors);
				break;
			case ']':
				CloseBracket(square_brackets, cursor_brackets, pos, i, errors);
				break;
			case '}':
				CloseBracket(curly_brackets, cursor_brackets, pos, i, errors);
				break;
			case '$': { // dollar symbol
				if (i + 1 >= len) {
					// we need more than just a dollar
					break;
				}
				// check if this is a dollar-quoted string
				idx_t next_dollar = 0;
				for (idx_t idx = i + 1; idx < len; idx++) {
					if (buf[idx] == '$') {
						// found the next dollar
						next_dollar = idx;
						break;
					}
					// all characters can be between A-Z, a-z or \200 - \377
					if (buf[idx] >= 'A' && buf[idx] <= 'Z') {
						continue;
					}
					if (buf[idx] >= 'a' && buf[idx] <= 'z') {
						continue;
					}
					if (buf[idx] >= '\200' && buf[idx] <= '\377') {
						continue;
					}
					// the first character CANNOT be a numeric, only subsequent characters
					if (idx > i + 1 && buf[idx] >= '0' && buf[idx] <= '9') {
						continue;
					}
					// not a dollar quoted string
					break;
				}
				if (next_dollar == 0) {
					// not a dollar quoted string
					break;
				}
				// dollar quoted string
				state = ScanState::DOLLAR_QUOTED_STRING;
				quote_pos = i;
				i = next_dollar;
				if (i < len) {
					// found a complete marker - store it
					idx_t marker_start = quote_pos + 1;
					dollar_quote_marker = string(buf + marker_start, i - marker_start);
				}
				break;
			}
			default:
				break;
			}
			break;
		case ScanState::IN_COMMENT:
			// comment state - the only thing that will get us out is a newline
			switch (c) {
			case '\r':
			case '\n':
				// newline - left comment state
				state = ScanState::STANDARD;
				comment_end.push_back(i);
				break;
			default:
				break;
			}
			break;
		case ScanState::IN_SINGLE_QUOTE:
			// single quote - all that will get us out is an unescaped single-quote
			if (c == '\'') {
				if (i + 1 < len && buf[i + 1] == '\'') {
					// double single-quote means the quote is escaped - continue
					i++;
					break;
				} else {
					// otherwise revert to standard scan state
					state = ScanState::STANDARD;
					break;
				}
			}
			break;
		case ScanState::IN_DOUBLE_QUOTE:
			// double quote - all that will get us out is an unescaped quote
			if (c == '"') {
				if (i + 1 < len && buf[i + 1] == '"') {
					// double quote means the quote is escaped - continue
					i++;
					break;
				} else {
					// otherwise revert to standard scan state
					state = ScanState::STANDARD;
					break;
				}
			}
			break;
		case ScanState::DOLLAR_QUOTED_STRING: {
			// dollar-quoted string - all that will get us out is a $[marker]$
			if (c != '$') {
				break;
			}
			if (i + 1 >= len) {
				// no room for the final dollar
				break;
			}
			// skip to the next dollar symbol
			idx_t start = i + 1;
			idx_t end = start;
			while (end < len && buf[end] != '$') {
				end++;
			}
			if (end >= len) {
				// no final dollar found - continue as normal
				break;
			}
			if (end - start != dollar_quote_marker.size()) {
				// length mismatch - cannot match
				break;
			}
			if (memcmp(buf + start, dollar_quote_marker.c_str(), dollar_quote_marker.size()) != 0) {
				// marker mismatch
				break;
			}
			// marker found! revert to standard state
			dollar_quote_marker = string();
			state = ScanState::STANDARD;
			i = end;
			break;
		}
		default:
			break;
		}
	}
	if (state == ScanState::IN_DOUBLE_QUOTE || state == ScanState::IN_SINGLE_QUOTE ||
	    state == ScanState::DOLLAR_QUOTED_STRING) {
		// quote is never closed
		errors.push_back(quote_pos);
	}
	HandleBracketErrors(brackets, errors);
	HandleBracketErrors(square_brackets, errors);
	HandleBracketErrors(curly_brackets, errors);

	// insert all the errors for highlighting
	for (auto &error : errors) {
		Linenoise::Log("Error found at position %llu\n", error);
		if (error < render_start || error > render_end) {
			continue;
		}
		auto render_error = error - render_start;
		InsertToken(tokenType::TOKEN_ERROR, render_error, tokens);
	}
	if (cursor_brackets.size() != 2) {
		// no matching cursor brackets found
		cursor_brackets.clear();
	}
	// insert bracket for highlighting
	for (auto &bracket_position : cursor_brackets) {
		Linenoise::Log("Highlight bracket at position %d\n", bracket_position);
		if (bracket_position < render_start || bracket_position > render_end) {
			continue;
		}

		idx_t render_position = bracket_position - render_start;
		InsertToken(tokenType::TOKEN_BRACKET, render_position, tokens);
	}
	// insert comments
	if (!comment_start.empty()) {
		vector<highlightToken> new_tokens;
		new_tokens.reserve(tokens.size());
		idx_t token_idx = 0;
		for (idx_t c = 0; c < comment_start.size(); c++) {
			auto c_start = comment_start[c];
			auto c_end = c < comment_end.size() ? comment_end[c] : len;
			if (c_start < render_start || c_end > render_end) {
				continue;
			}
			Linenoise::Log("Comment at position %d to %d\n", c_start, c_end);
			c_start -= render_start;
			c_end -= render_start;
			bool inserted_comment = false;

			highlightToken comment_token;
			comment_token.start = c_start;
			comment_token.type = tokenType::TOKEN_COMMENT;
			comment_token.search_match = false;

			for (; token_idx < tokens.size(); token_idx++) {
				if (tokens[token_idx].start >= c_start) {
					// insert the comment here
					new_tokens.push_back(comment_token);
					inserted_comment = true;
					break;
				}
				new_tokens.push_back(tokens[token_idx]);
			}
			if (!inserted_comment) {
				new_tokens.push_back(comment_token);
			} else {
				// skip all tokens until we exit the comment again
				for (; token_idx < tokens.size(); token_idx++) {
					if (tokens[token_idx].start > c_end) {
						break;
					}
				}
			}
		}
		for (; token_idx < tokens.size(); token_idx++) {
			new_tokens.push_back(tokens[token_idx]);
		}
		tokens = std::move(new_tokens);
	}
}

bool IsCompletionCharacter(char c) {
	if (c >= 'A' && c <= 'Z') {
		return true;
	}
	if (c >= 'a' && c <= 'z') {
		return true;
	}
	if (c == '_') {
		return true;
	}
	return false;
}

bool Linenoise::AddCompletionMarker(const char *buf, idx_t len, string &result_buffer,
                                    vector<highlightToken> &tokens) const {
	if (!enableCompletionRendering) {
		return false;
	}
	if (!continuation_markers) {
		// don't render when pressing ctrl+c, only when editing
		return false;
	}
	static constexpr const idx_t MAX_COMPLETION_LENGTH = 1000;
	if (len >= MAX_COMPLETION_LENGTH) {
		return false;
	}
	if (!insert || pos != len) {
		// only show when inserting a character at the end
		return false;
	}
	if (pos < 3) {
		// we need at least 3 bytes
		return false;
	}
	if (!tokens.empty() && tokens.back().type == tokenType::TOKEN_ERROR) {
		// don't show auto-completion when we have errors
		return false;
	}
	// we ONLY show completion if we have typed at least three characters that are supported for completion
	// for now this is ONLY the characters a-z, A-Z and underscore (_)
	for (idx_t i = pos - 3; i < pos; i++) {
		if (!IsCompletionCharacter(buf[i])) {
			return false;
		}
	}
	auto completion = TabComplete();
	if (completion.completions.empty()) {
		// no completions found
		return false;
	}
	if (completion.completions[0].completion.size() <= len) {
		// completion is not long enough
		return false;
	}
	// we have stricter requirements for rendering completions - the completion must match exactly
	for (idx_t i = pos; i > 0; i--) {
		auto cpos = i - 1;
		if (!IsCompletionCharacter(buf[cpos])) {
			break;
		}
		if (completion.completions[0].completion[cpos] != buf[cpos]) {
			return false;
		}
	}
	// add the first completion found for rendering purposes
	result_buffer = string(buf, len);
	result_buffer += completion.completions[0].completion.substr(len);

	highlightToken completion_token;
	completion_token.start = len;
	completion_token.type = tokenType::TOKEN_COMMENT;
	completion_token.search_match = true;
	tokens.push_back(completion_token);
	return true;
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
	int fd = ofd;
	std::string highlight_buffer;
	auto render_buf = this->buf;
	auto render_len = this->len;
	idx_t render_start = 0;
	idx_t render_end = render_len;
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
		if (y_scroll == 0) {
			render_start = 0;
		} else {
			render_start = ColAndRowToPosition(y_scroll, 0);
		}
		if (int(y_scroll) + int(ws.ws_row) >= rows) {
			render_end = len;
		} else {
			render_end = ColAndRowToPosition(y_scroll + ws.ws_row, 99999);
		}
		new_cursor_row -= y_scroll;
		render_buf += render_start;
		render_len = render_end - render_start;
		Linenoise::Log("truncate to rows %d - %d (render bytes %d to %d)", y_scroll, y_scroll + ws.ws_row, render_start,
		               render_end);
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
		bool is_dot_command = buf[0] == '.';
		auto match = search_index < search_matches.size() ? &search_matches[search_index] : nullptr;
		tokens = Highlighting::Tokenize(render_buf, render_len, is_dot_command, match);

		// add error highlighting
		AddErrorHighlighting(render_start, render_end, tokens);

		// add completion hint
		if (AddCompletionMarker(render_buf, render_len, highlight_buffer, tokens)) {
			render_buf = (char *)highlight_buffer.c_str();
			render_len = highlight_buffer.size();
		}
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
		Linenoise::Log("go down %d", old_rows - old_cursor_rows);
		snprintf(seq, 64, "\x1b[%dB", old_rows - int(old_cursor_rows));
		append_buffer.Append(seq);
	}

	/* Now for every row clear it, go up. */
	for (int j = 0; j < old_rows - 1; j++) {
		Linenoise::Log("clear+up");
		append_buffer.Append("\r\x1b[0K\x1b[1A");
	}

	/* Clean the top line. */
	Linenoise::Log("clear");
	append_buffer.Append("\r\x1b[0K");

	/* Write the prompt and the current buffer content */
	if (y_scroll == 0) {
		append_buffer.Append(prompt);
	}
	append_buffer.Append(render_buf, render_len);

	/* Show hints if any. */
	RefreshShowHints(append_buffer, plen);

	/* If we are at the very end of the screen with our prompt, we need to
	 * emit a newline and move the prompt to the first column. */
	Linenoise::Log("pos > 0 %d", pos > 0 ? 1 : 0);
	Linenoise::Log("pos == len %d", pos == len ? 1 : 0);
	Linenoise::Log("new_cursor_x == cols %d", new_cursor_x == ws.ws_col ? 1 : 0);
	if (pos > 0 && pos == len && new_cursor_x == ws.ws_col) {
		Linenoise::Log("<newline>", 0);
		append_buffer.Append("\n");
		append_buffer.Append("\r");
		rows++;
		new_cursor_row++;
		new_cursor_x = 0;
		if (rows > (int)maxrows) {
			maxrows = rows;
		}
	}
	Linenoise::Log("render %d rows (old rows %d)", rows, old_rows);

	/* Move cursor to right position. */
	Linenoise::Log("new_cursor_row %d", new_cursor_row);
	Linenoise::Log("new_cursor_x %d", new_cursor_x);
	Linenoise::Log("len %d", len);
	Linenoise::Log("old_cursor_rows %d", old_cursor_rows);
	Linenoise::Log("pos %d", pos);
	Linenoise::Log("max cols %d", ws.ws_col);

	/* Go up till we reach the expected positon. */
	if (rows - new_cursor_row > 0) {
		Linenoise::Log("go-up %d", rows - new_cursor_row);
		snprintf(seq, 64, "\x1b[%dA", rows - new_cursor_row);
		append_buffer.Append(seq);
	}

	/* Set column. */
	col = new_cursor_x;
	Linenoise::Log("set col %d", 1 + col);
	if (col) {
		snprintf(seq, 64, "\r\x1b[%dC", col);
	} else {
		snprintf(seq, 64, "\r");
	}
	append_buffer.Append(seq);

	Linenoise::Log("\n");
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
