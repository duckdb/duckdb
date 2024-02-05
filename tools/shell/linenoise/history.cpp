#include "history.hpp"
#include "linenoise.hpp"
#include "terminal.hpp"
#include "duckdb_shell_wrapper.h"
#include "sqlite3.h"
#include <sys/stat.h>

namespace duckdb {
#define LINENOISE_DEFAULT_HISTORY_MAX_LEN 1000
static idx_t history_max_len = LINENOISE_DEFAULT_HISTORY_MAX_LEN;
static idx_t history_len = 0;
static char **history = nullptr;
static char *history_file = nullptr;

/* Free the history, but does not reset it. Only used when we have to
 * exit() to avoid memory leaks are reported by valgrind & co. */
void History::Free() {
	if (history) {
		for (idx_t j = 0; j < history_len; j++) {
			free(history[j]);
		}
		free(history);
	}
}

idx_t History::GetLength() {
	return history_len;
}

const char *History::GetEntry(idx_t index) {
	if (!history || index >= history_len) {
		// FIXME: print debug message
		return "";
	}
	return history[index];
}

void History::Overwrite(idx_t index, const char *new_entry) {
	if (!history || index >= history_len) {
		// FIXME: print debug message
		return;
	}

	free(history[index]);
	history[index] = strdup(new_entry);
}

void History::RemoveLastEntry() {
	history_len--;
	free(history[history_len]);
}

int History::Add(const char *line) {
	char *linecopy;

	if (history_max_len == 0) {
		return 0;
	}

	/* Initialization on first call. */
	if (history == nullptr) {
		history = (char **)malloc(sizeof(char *) * history_max_len);
		if (history == nullptr) {
			return 0;
		}
		memset(history, 0, (sizeof(char *) * history_max_len));
	}

	/* Don't add duplicated lines. */
	if (history_len && !strcmp(history[history_len - 1], line)) {
		return 0;
	}

	/* Add an heap allocated copy of the line in the history.
	 * If we reached the max length, remove the older line. */
	linecopy = strdup(line);
	if (!linecopy) {
		return 0;
	}
	if (!Terminal::IsMultiline()) {
		// replace all newlines with spaces
		for (auto ptr = linecopy; *ptr; ptr++) {
			if (*ptr == '\n' || *ptr == '\r') {
				*ptr = ' ';
			}
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
		if (fp == nullptr) {
			return 1;
		}
		fprintf(fp, "%s\n", line);
		fclose(fp);
	}
	return 1;
}

int History::SetMaxLength(idx_t len) {
	char **new_entry;

	if (len < 1) {
		return 0;
	}
	if (history) {
		idx_t tocopy = history_len;

		new_entry = (char **)malloc(sizeof(char *) * len);
		if (new_entry == nullptr) {
			return 0;
		}

		/* If we can't copy everything, free the elements we'll not use. */
		if (len < tocopy) {
			for (idx_t j = 0; j < tocopy - len; j++) {
				free(history[j]);
			}
			tocopy = len;
		}
		memset(new_entry, 0, sizeof(char *) * len);
		memcpy(new_entry, history + (history_len - tocopy), sizeof(char *) * tocopy);
		free(history);
		history = new_entry;
	}
	history_max_len = len;
	if (history_len > history_max_len) {
		history_len = history_max_len;
	}
	return 1;
}

int History::Save(const char *filename) {
	mode_t old_umask = umask(S_IXUSR | S_IRWXG | S_IRWXO);
	FILE *fp;

	fp = fopen(filename, "w");
	umask(old_umask);
	if (fp == nullptr) {
		return -1;
	}
	chmod(filename, S_IRUSR | S_IWUSR);
	for (idx_t j = 0; j < history_len; j++) {
		fprintf(fp, "%s\n", history[j]);
	}
	fclose(fp);
	return 0;
}

int History::Load(const char *filename) {
	FILE *fp = fopen(filename, "r");
	char buf[LINENOISE_MAX_LINE + 1];
	buf[LINENOISE_MAX_LINE] = '\0';

	if (fp == nullptr) {
		return -1;
	}

	std::string result;
	while (fgets(buf, LINENOISE_MAX_LINE, fp) != nullptr) {
		char *p;

		// strip the newline first
		p = strchr(buf, '\r');
		if (!p) {
			p = strchr(buf, '\n');
		}
		if (p) {
			*p = '\0';
		}
		if (result.empty() && buf[0] == '.') {
			// if the first character is a dot this is a dot command
			// add the full line to the history
			History::Add(buf);
			continue;
		}
		// else we are parsing a SQL statement
		result += buf;
		if (sqlite3_complete(result.c_str())) {
			// this line contains a full SQL statement - add it to the history
			History::Add(result.c_str());
			result = std::string();
			continue;
		}
		// the result does not contain a full SQL statement - add a newline deliminator and move on to the next line
		result += "\r\n";
	}
	fclose(fp);

	history_file = strdup(filename);
	return 0;
}

} // namespace duckdb
