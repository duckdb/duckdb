#include "linenoise.hpp"
#include "linenoise.h"
#include "history.hpp"
#include "terminal.hpp"

using duckdb::History;
using duckdb::idx_t;
using duckdb::Linenoise;
using duckdb::Terminal;

/* The high level function that is the main API of the linenoise library.
 * This function checks if the terminal has basic capabilities, just checking
 * for a blacklist of stupid terminals, and later either calls the line
 * editing function or uses dummy fgets() so that you will be able to type
 * something even in the most desperate of the conditions. */
char *linenoise(const char *prompt) {
	char buf[LINENOISE_MAX_LINE];
	int count;

	if (!Terminal::IsAtty()) {
		/* Not a tty: read from file / pipe. In this mode we don't want any
		 * limit to the line size, so we call a function to handle that. */
		return Terminal::EditNoTTY();
	} else if (Terminal::IsUnsupportedTerm()) {
		size_t len;

		printf("%s", prompt);
		fflush(stdout);
		if (fgets(buf, LINENOISE_MAX_LINE, stdin) == NULL) {
			return NULL;
		}
		len = strlen(buf);
		while (len && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
			len--;
			buf[len] = '\0';
		}
		return strdup(buf);
	} else {
		count = Terminal::EditRaw(buf, LINENOISE_MAX_LINE, prompt);
		if (count == -1) {
			return NULL;
		}
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

/* This is the API call to add a new entry in the linenoise history.
 * It uses a fixed array of char pointers that are shifted (memmoved)
 * when the history max length is reached in order to remove the older
 * entry and make room for the new one, so it is not exactly suitable for huge
 * histories, but will work well for a few hundred of entries.
 *
 * Using a circular buffer is smarter, but a bit more complex to handle. */
int linenoiseHistoryAdd(const char *line) {
	return History::Add(line);
}

/* Set the maximum length for the history. This function can be called even
 * if there is already some history, the function will make sure to retain
 * just the latest 'len' elements if the new history length value is smaller
 * than the amount of items already inside the history. */
int linenoiseHistorySetMaxLen(int len) {
	if (len < 0) {
		return 0;
	}
	return History::SetMaxLength(idx_t(len));
}

/* Save the history in the specified file. On success 0 is returned
 * otherwise -1 is returned. */
int linenoiseHistorySave(const char *filename) {
	return History::Save(filename);
}

/* Load the history from the specified file. If the file does not exist
 * zero is returned and no operation is performed.
 *
 * If the file exists and the operation succeeded 0 is returned, otherwise
 * on error -1 is returned. */
int linenoiseHistoryLoad(const char *filename) {
	return History::Load(filename);
}

/* Register a callback function to be called for tab-completion. */
void linenoiseSetCompletionCallback(linenoiseCompletionCallback *fn) {
	Linenoise::SetCompletionCallback(fn);
}

/* Register a hits function to be called to show hits to the user at the
 * right of the prompt. */
void linenoiseSetHintsCallback(linenoiseHintsCallback *fn) {
	Linenoise::SetHintsCallback(fn);
}

/* Register a function to free the hints returned by the hints callback
 * registered with linenoiseSetHintsCallback(). */
void linenoiseSetFreeHintsCallback(linenoiseFreeHintsCallback *fn) {
	Linenoise::SetFreeHintsCallback(fn);
}

void linenoiseSetMultiLine(int ml) {
	Terminal::SetMultiLine(ml);
}

void linenoiseSetPrompt(const char *continuation, const char *continuationSelected) {
	Linenoise::SetPrompt(continuation, continuationSelected);
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

size_t linenoiseComputeRenderWidth(const char *buf, size_t len) {
	return Linenoise::ComputeRenderWidth(buf, len);
}

int linenoiseGetRenderPosition(const char *buf, size_t len, int max_width, int *n) {
	return Linenoise::GetRenderPosition(buf, len, max_width, n);
}

void linenoiseClearScreen(void) {
	Terminal::ClearScreen();
}

int linenoiseParseOption(const char **azArg, int nArg, const char **out_error) {
	return Linenoise::ParseOption(azArg, nArg, out_error);
}
