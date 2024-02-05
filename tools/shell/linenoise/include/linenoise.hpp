//===----------------------------------------------------------------------===//
//                         DuckDB
//
// linenoise.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "terminal.hpp"
#include "linenoise.h"

#define LINENOISE_MAX_LINE 204800

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

namespace duckdb {
struct highlightToken;
struct AppendBuffer;

enum class HistoryScrollDirection : uint8_t { LINENOISE_HISTORY_NEXT, LINENOISE_HISTORY_PREV };

struct searchMatch {
	size_t history_index;
	size_t match_start;
	size_t match_end;
};

class Linenoise {
public:
	Linenoise(int stdin_fd, int stdout_fd, char *buf, size_t buflen, const char *prompt);

public:
	int Edit();

	static void SetCompletionCallback(linenoiseCompletionCallback *fn);
	static void SetHintsCallback(linenoiseHintsCallback *fn);
	static void SetFreeHintsCallback(linenoiseFreeHintsCallback *fn);

	static linenoiseHintsCallback *HintsCallback();
	static linenoiseFreeHintsCallback *FreeHintsCallback();

	static void SetPrompt(const char *continuation, const char *continuationSelected);
	static size_t ComputeRenderWidth(const char *buf, size_t len);
	static int GetRenderPosition(const char *buf, size_t len, int max_width, int *n);

	static int ParseOption(const char **azArg, int nArg, const char **out_error);

	int GetPromptWidth() const;

	void RefreshLine();
	int CompleteLine();
	void InsertCharacter(char c);
	int EditInsert(char c);
	int EditInsertMulti(const char *c);
	void EditMoveLeft();
	void EditMoveRight();
	void EditMoveWordLeft();
	void EditMoveWordRight();
	bool EditMoveRowUp();
	bool EditMoveRowDown();
	void EditMoveHome();
	void EditMoveEnd();
	void EditHistoryNext(HistoryScrollDirection dir);
	void EditDelete();
	void EditBackspace();
	void EditDeletePrevWord();

	void CancelSearch();
	char AcceptSearch(char nextCommand);
	void PerformSearch();
	void SearchPrev();
	void SearchNext();

	char Search(char c);

	void RefreshMultiLine();
	void RefreshSingleLine() const;
	void RefreshSearch();
	void RefreshShowHints(AppendBuffer &append_buffer, int plen) const;

	size_t PrevChar() const;
	size_t NextChar() const;

	void NextPosition(const char *buf, size_t len, size_t &cpos, int &rows, int &cols, int plen) const;
	void PositionToColAndRow(size_t target_pos, int &out_row, int &out_col, int &rows, int &cols) const;
	size_t ColAndRowToPosition(int target_row, int target_col) const;

	string AddContinuationMarkers(const char *buf, size_t len, int plen, int cursor_row,
	                              vector<highlightToken> &tokens) const;

	static bool IsNewline(char c);
	static bool IsWordBoundary(char c);
	static bool AllWhitespace(const char *z);

public:
	int ifd;                                 /* Terminal stdin file descriptor. */
	int ofd;                                 /* Terminal stdout file descriptor. */
	char *buf;                               /* Edited line buffer. */
	size_t buflen;                           /* Edited line buffer size. */
	const char *prompt;                      /* Prompt to display. */
	size_t plen;                             /* Prompt length. */
	size_t pos;                              /* Current cursor position. */
	size_t old_cursor_rows;                  /* Previous refresh cursor position. */
	size_t len;                              /* Current edited line length. */
	size_t y_scroll;                         /* The y scroll position (multiline mode) */
	TerminalSize ws;                         /* Terminal size */
	size_t maxrows;                          /* Maximum num of rows used so far (multiline mode) */
	idx_t history_index;                     /* The history index we are currently editing. */
	bool clear_screen;                       /* Whether we are clearing the screen */
	bool continuation_markers;               /* Whether or not to render continuation markers */
	bool search;                             /* Whether or not we are searching our history */
	bool render;                             /* Whether or not to re-render */
	bool has_more_data;                      /* Whether or not there is more data available in the buffer (copy+paste)*/
	std::string search_buf;                  //! The search buffer
	std::vector<searchMatch> search_matches; //! The set of search matches in our history
	size_t search_index;                     //! The current match index
};

} // namespace duckdb
