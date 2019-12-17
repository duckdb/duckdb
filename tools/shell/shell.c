/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement the "sqlite" command line
** utility for accessing SQLite databases.
*/

#include "sqlite3.h"

/*
** Enable large-file support for fopen() and friends on unix.
*/
#ifndef SQLITE_DISABLE_LFS
#define _LARGE_FILE 1
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#define _LARGEFILE_SOURCE 1
#endif

// TODO
#include "shell_config.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
typedef sqlite3_int64 i64;
typedef sqlite3_uint64 u64;
typedef unsigned char u8;
#include <ctype.h>
#include <stdarg.h>

#if !defined(_WIN32) && !defined(WIN32)
#include <signal.h>
#if !defined(__RTP__) && !defined(_WRS_KERNEL)
#include <pwd.h>
#endif
#endif

#if (!defined(_WIN32) && !defined(WIN32)) || defined(__MINGW32__)
#include <dirent.h>
#include <unistd.h>
#if defined(__MINGW32__)
#define DIRENT dirent
#ifndef S_ISLNK
#define S_ISLNK(mode) (0)
#endif
#endif
#endif

#include <sys/stat.h>
#include <sys/types.h>

#if HAVE_READLINE
#include <readline/history.h>
#include <readline/readline.h>
#endif

#if HAVE_EDITLINE
#include <editline/readline.h>
#endif

#if HAVE_EDITLINE || HAVE_READLINE

#define shell_add_history(X) add_history(X)
#define shell_read_history(X) read_history(X)
#define shell_write_history(X) write_history(X)
#define shell_stifle_history(X) stifle_history(X)
#define shell_readline(X) readline(X)

#elif HAVE_LINENOISE

#include "linenoise.h"
#define shell_add_history(X) linenoiseHistoryAdd(X)
#define shell_read_history(X) linenoiseHistoryLoad(X)
#define shell_write_history(X) linenoiseHistorySave(X)
#define shell_stifle_history(X) linenoiseHistorySetMaxLen(X)
#define shell_readline(X) linenoise(X)

#else

#define shell_read_history(X)
#define shell_write_history(X)
#define shell_stifle_history(X)

#define SHELL_USE_LOCAL_GETLINE 1
#endif

/* ctype macros that work with signed characters */
#define IsSpace(X) isspace((unsigned char)X)
#define IsDigit(X) isdigit((unsigned char)X)
#define ToLower(X) (char)tolower((unsigned char)X)

#define setBinaryMode(X, Y)
#define setTextMode(X, Y)

/*
** Used to prevent warnings about unused parameters
*/
#define UNUSED_PARAMETER(x) (void)(x)

/*
** Number of elements in an array
*/
#define ArraySize(X) (int)(sizeof(X) / sizeof(X[0]))

/*
** If the following flag is set, then command execution stops
** at an error if we are not interactive.
*/
static int bail_on_error = 0;

/*
** Threat stdin as an interactive input if the following variable
** is true.  Otherwise, assume stdin is connected to a file or pipe.
*/
static int stdin_is_interactive = 1;

/*
** On Windows systems we have to know if standard output is a console
** in order to translate UTF-8 into MBCS.  The following variable is
** true if translation is required.
*/
static int stdout_is_console = 1;

/*
** The following is the open SQLite database.  We make a pointer
** to this database a static variable so that it can be accessed
** by the SIGINT handler to interrupt database processing.
*/
static sqlite3 *globalDb = 0;

/*
** True if an interrupt (Control-C) has been received.
*/
static volatile int seenInterrupt = 0;

/*
** This is the name of our program. It is set in main(), used
** in a number of other places, mostly for error messages.
*/
static char *Argv0;

/*
** Prompt strings. Initialized in main. Settable with
**   .prompt main continue
*/
static char mainPrompt[20];     /* First line prompt. default: "sqlite> "*/
static char continuePrompt[20]; /* Continuation prompt. default: "   ...> " */

#define utf8_printf fprintf
#define raw_printf fprintf

/*
** Output string zUtf to stream pOut as w characters.  If w is negative,
** then right-justify the text.  W is the width in UTF-8 characters, not
** in bytes.  This is different from the %*.*s specification in printf
** since with %*.*s the width is measured in bytes, not characters.
*/
static void utf8_width_print(FILE *pOut, int w, const char *zUtf) {
	int i;
	int n;
	int aw = w < 0 ? -w : w;
	char zBuf[1000];
	if (aw > (int)sizeof(zBuf) / 3)
		aw = (int)sizeof(zBuf) / 3;
	for (i = n = 0; zUtf[i]; i++) {
		if ((zUtf[i] & 0xc0) != 0x80) {
			n++;
			if (n == aw) {
				do {
					i++;
				} while ((zUtf[i] & 0xc0) == 0x80);
				break;
			}
		}
	}
	if (n >= aw) {
		utf8_printf(pOut, "%.*s", i, zUtf);
	} else if (w < 0) {
		utf8_printf(pOut, "%*s%s", aw - n, "", zUtf);
	} else {
		utf8_printf(pOut, "%s%*s", zUtf, aw - n, "");
	}
}

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
*/
static int strlen30(const char *z) {
	const char *z2 = z;
	while (*z2) {
		z2++;
	}
	return 0x3fffffff & (int)(z2 - z);
}

/*
** Return the length of a string in characters.  Multibyte UTF8 characters
** count as a single character.
*/
static int strlenChar(const char *z) {
	int n = 0;
	while (*z) {
		if ((0xc0 & *(z++)) != 0x80)
			n++;
	}
	return n;
}

/*
** This routine reads a line of text from FILE in, stores
** the text in memory obtained from malloc() and returns a pointer
** to the text.  NULL is returned at end of file, or if malloc()
** fails.
**
** If zLine is not NULL then it is a malloced buffer returned from
** a previous call to this routine that may be reused.
*/
static char *local_getline(char *zLine, FILE *in) {
	int nLine = zLine == 0 ? 0 : 100;
	int n = 0;

	while (1) {
		if (n + 100 > nLine) {
			nLine = nLine * 2 + 100;
			zLine = realloc(zLine, nLine);
			if (zLine == 0)
				return 0;
		}
		if (fgets(&zLine[n], nLine - n, in) == 0) {
			if (n == 0) {
				free(zLine);
				return 0;
			}
			zLine[n] = 0;
			break;
		}
		while (zLine[n])
			n++;
		if (n > 0 && zLine[n - 1] == '\n') {
			n--;
			if (n > 0 && zLine[n - 1] == '\r')
				n--;
			zLine[n] = 0;
			break;
		}
	}
	return zLine;
}

/*
** Retrieve a single line of input text.
**
** If in==0 then read from standard input and prompt before each line.
** If isContinuation is true, then a continuation prompt is appropriate.
** If isContinuation is zero, then the main prompt should be used.
**
** If zPrior is not NULL then it is a buffer from a prior call to this
** routine that can be reused.
**
** The result is stored in space obtained from malloc() and must either
** be freed by the caller or else passed back into this routine via the
** zPrior argument for reuse.
*/
static char *one_input_line(FILE *in, char *zPrior, int isContinuation) {
	char *zPrompt;
	char *zResult;
	if (in != 0) {
		zResult = local_getline(zPrior, in);
	} else {
		zPrompt = isContinuation ? continuePrompt : mainPrompt;
#if SHELL_USE_LOCAL_GETLINE
		printf("%s", zPrompt);
		fflush(stdout);
		zResult = local_getline(zPrior, stdin);
#else
		free(zPrior);
		zResult = shell_readline(zPrompt);
		if (zResult && *zResult)
			shell_add_history(zResult);
#endif
	}
	return zResult;
}

/*
** Return the value of a hexadecimal digit.  Return -1 if the input
** is not a hex digit.
*/
static int hexDigitValue(char c) {
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return -1;
}

/*
** Interpret zArg as an integer value, possibly with suffixes.
*/
static sqlite3_int64 integerValue(const char *zArg) {
	sqlite3_int64 v = 0;
	static const struct {
		char *zSuffix;
		int iMult;
	} aMult[] = {
	    {"KiB", 1024}, {"MiB", 1024 * 1024}, {"GiB", 1024 * 1024 * 1024},
	    {"KB", 1000},  {"MB", 1000000},      {"GB", 1000000000},
	    {"K", 1000},   {"M", 1000000},       {"G", 1000000000},
	};
	int i;
	int isNeg = 0;
	if (zArg[0] == '-') {
		isNeg = 1;
		zArg++;
	} else if (zArg[0] == '+') {
		zArg++;
	}
	if (zArg[0] == '0' && zArg[1] == 'x') {
		int x;
		zArg += 2;
		while ((x = hexDigitValue(zArg[0])) >= 0) {
			v = (v << 4) + x;
			zArg++;
		}
	} else {
		while (IsDigit(zArg[0])) {
			v = v * 10 + zArg[0] - '0';
			zArg++;
		}
	}
	for (i = 0; i < ArraySize(aMult); i++) {
		if (sqlite3_stricmp(aMult[i].zSuffix, zArg) == 0) {
			v *= aMult[i].iMult;
			break;
		}
	}
	return isNeg ? -v : v;
}

/*
** A variable length string to which one can append text.
*/
typedef struct ShellText ShellText;
struct ShellText {
	char *z;
	int n;
	int nAlloc;
};

/* All SQLite keywords, in alphabetical order */
const char *azKeywords[] = {
    "ABORT",      "ACTION",    "ADD",         "AFTER",        "ALL",          "ALTER",
    "ANALYZE",    "AND",       "AS",          "ASC",          "ATTACH",       "AUTOINCREMENT",
    "BEFORE",     "BEGIN",     "BETWEEN",     "BY",           "CASCADE",      "CASE",
    "CAST",       "CHECK",     "COLLATE",     "COLUMN",       "COMMIT",       "CONFLICT",
    "CONSTRAINT", "CREATE",    "CROSS",       "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "DATABASE",   "DEFAULT",   "DEFERRABLE",  "DEFERRED",     "DELETE",       "DESC",
    "DETACH",     "DISTINCT",  "DROP",        "EACH",         "ELSE",         "END",
    "ESCAPE",     "EXCEPT",    "EXCLUSIVE",   "EXISTS",       "EXPLAIN",      "FAIL",
    "FOR",        "FOREIGN",   "FROM",        "FULL",         "GLOB",         "GROUP",
    "HAVING",     "IF",        "IGNORE",      "IMMEDIATE",    "IN",           "INDEX",
    "INDEXED",    "INITIALLY", "INNER",       "INSERT",       "INSTEAD",      "INTERSECT",
    "INTO",       "IS",        "ISNULL",      "JOIN",         "KEY",          "LEFT",
    "LIKE",       "LIMIT",     "MATCH",       "NATURAL",      "NO",           "NOT",
    "NOTNULL",    "NULL",      "OF",          "OFFSET",       "ON",           "OR",
    "ORDER",      "OUTER",     "PLAN",        "PRAGMA",       "PRIMARY",      "QUERY",
    "RAISE",      "RECURSIVE", "REFERENCES",  "REGEXP",       "REINDEX",      "RELEASE",
    "RENAME",     "REPLACE",   "RESTRICT",    "RIGHT",        "ROLLBACK",     "ROW",
    "SAVEPOINT",  "SELECT",    "SET",         "TABLE",        "TEMP",         "TEMPORARY",
    "THEN",       "TO",        "TRANSACTION", "TRIGGER",      "UNION",        "UNIQUE",
    "UPDATE",     "USING",     "VACUUM",      "VALUES",       "VIEW",         "VIRTUAL",
    "WHEN",       "WHERE",     "WITH",        "WITHOUT",
};

/*
** Attempt to determine if identifier zName needs to be quoted, either
** because it contains non-alphanumeric characters, or because it is an
** SQLite keyword.  Be conservative in this estimate:  When in doubt assume
** that quoting is required.
**
** Return '"' if quoting is required.  Return 0 if no quoting is required.
*/

static char quoteChar(const char *zName) {
	int i, lwr, upr, mid, c;
	if (!isalpha((unsigned char)zName[0]) && zName[0] != '_')
		return '"';
	for (i = 0; zName[i]; i++) {
		if (!isalnum((unsigned char)zName[i]) && zName[i] != '_')
			return '"';
	}
	lwr = 0;
	upr = sizeof(azKeywords) / sizeof(azKeywords[0]) - 1;
	while (lwr <= upr) {
		mid = (lwr + upr) / 2;
		c = sqlite3_stricmp(azKeywords[mid], zName);
		if (c == 0)
			return '"';
		if (c < 0) {
			lwr = mid + 1;
		} else {
			upr = mid - 1;
		}
	}
	return 0;
}

/*
** State information about the database connection is contained in an
** instance of the following structure.
*/
typedef struct ShellState ShellState;
struct ShellState {
	sqlite3 *db;                /* The database */
	u8 autoExplain;             /* Automatically turn on .explain mode */
	u8 autoEQP;                 /* Run EXPLAIN QUERY PLAN prior to seach SQL stmt */
	u8 statsOn;                 /* True to display memory stats before each finalize */
	u8 scanstatsOn;             /* True to display scan stats before each finalize */
	u8 openMode;                /* SHELL_OPEN_NORMAL, _APPENDVFS, or _ZIPFILE */
	u8 doXdgOpen;               /* Invoke start/open/xdg-open in output_reset() */
	int outCount;               /* Revert to stdout when reaching zero */
	int cnt;                    /* Number of records displayed so far */
	FILE *out;                  /* Write results here */
	FILE *traceOut;             /* Output for sqlite3_trace() */
	int nErr;                   /* Number of errors seen */
	int mode;                   /* An output mode setting */
	int modePrior;              /* Saved mode */
	int cMode;                  /* temporary output mode for the current query */
	int normalMode;             /* Output mode before ".explain on" */
	int writableSchema;         /* True if PRAGMA writable_schema=ON */
	int showHeader;             /* True to show column names in List or Column mode */
	int nCheck;                 /* Number of ".check" commands run */
	unsigned shellFlgs;         /* Various flags */
	char *zDestTable;           /* Name of destination table when MODE_Insert */
	char *zTempFile;            /* Temporary file that might need deleting */
	char zTestcase[30];         /* Name of current test case */
	char colSeparator[20];      /* Column separator character for several modes */
	char rowSeparator[20];      /* Row separator character for MODE_Ascii */
	char colSepPrior[20];       /* Saved column separator */
	char rowSepPrior[20];       /* Saved row separator */
	int colWidth[100];          /* Requested width of each column when in column mode*/
	int actualWidth[100];       /* Actual width of each column */
	char nullValue[20];         /* The text to print when a NULL comes back from
	                             ** the database */
	char outfile[FILENAME_MAX]; /* Filename for *out */
	const char *zDbFilename;    /* name of the database file */
	char *zFreeOnClose;         /* Filename to free when closing */
	const char *zVfs;           /* Name of VFS to use */
	sqlite3_stmt *pStmt;        /* Current statement if any. */
	FILE *pLog;                 /* Write log output here */
	int *aiIndent;              /* Array of indents used in MODE_Explain */
	int nIndent;                /* Size of array aiIndent[] */
	int iIndent;                /* Index of current op in aiIndent[] */
#if defined(SQLITE_ENABLE_SESSION)
	int nSession;            /* Number of active sessions */
	OpenSession aSession[4]; /* Array of sessions.  [0] is in focus. */
#endif
};

/* Allowed values for ShellState.autoEQP
 */
#define AUTOEQP_off 0
#define AUTOEQP_on 1
#define AUTOEQP_trigger 2
#define AUTOEQP_full 3

/* Allowed values for ShellState.openMode
 */
#define SHELL_OPEN_UNSPEC 0    /* No open-mode specified */
#define SHELL_OPEN_NORMAL 1    /* Normal database file */
#define SHELL_OPEN_APPENDVFS 2 /* Use appendvfs */
#define SHELL_OPEN_ZIPFILE 3   /* Use the zipfile virtual table */
#define SHELL_OPEN_READONLY 4  /* Open a normal database read-only */

/*
** These are the allowed shellFlgs values
*/
#define SHFLG_Pagecache 0x00000001     /* The --pagecache option is used */
#define SHFLG_Lookaside 0x00000002     /* Lookaside memory is used */
#define SHFLG_Backslash 0x00000004     /* The --backslash option is used */
#define SHFLG_PreserveRowid 0x00000008 /* .dump preserves rowid values */
#define SHFLG_Newlines 0x00000010      /* .dump --newline flag */
#define SHFLG_CountChanges 0x00000020  /* .changes setting */
#define SHFLG_Echo 0x00000040          /* .echo or --echo setting */

/*
** Macros for testing and setting shellFlgs
*/
#define ShellHasFlag(P, X) (((P)->shellFlgs & (X)) != 0)
#define ShellSetFlag(P, X) ((P)->shellFlgs |= (X))
#define ShellClearFlag(P, X) ((P)->shellFlgs &= (~(X)))

/*
** These are the allowed modes.
*/
#define MODE_Line 0    /* One column per line.  Blank line between records */
#define MODE_Column 1  /* One record per line in neat columns */
#define MODE_List 2    /* One record per line with a separator */
#define MODE_Semi 3    /* Same as MODE_List but append ";" to each line */
#define MODE_Html 4    /* Generate an XHTML table */
#define MODE_Insert 5  /* Generate SQL "insert" statements */
#define MODE_Quote 6   /* Quote values as for SQL */
#define MODE_Tcl 7     /* Generate ANSI-C or TCL quoted elements */
#define MODE_Csv 8     /* Quote strings, numbers are plain */
#define MODE_Explain 9 /* Like MODE_Column, but do not truncate data */
#define MODE_Ascii 10  /* Use ASCII unit and record separators (0x1F/0x1E) */
#define MODE_Pretty 11 /* Pretty-print schemas */

static const char *modeDescr[] = {
    "line", "column", "list", "semi", "html", "insert", "quote", "tcl", "csv", "explain", "ascii", "prettyprint",
};

/*
** These are the column/row/line separators used by the various
** import/export modes.
*/
#define SEP_Column "|"
#define SEP_Row "\n"
#define SEP_Tab "\t"
#define SEP_Space " "
#define SEP_Comma ","
#define SEP_CrLf "\r\n"
#define SEP_Unit "\x1F"
#define SEP_Record "\x1E"

/*
** A callback for the sqlite3_log() interface.
*/
static void shellLog(void *pArg, int iErrCode, const char *zMsg) {
	ShellState *p = (ShellState *)pArg;
	if (p->pLog == 0)
		return;
	utf8_printf(p->pLog, "(%d) %s\n", iErrCode, zMsg);
	fflush(p->pLog);
}

/*
** Save or restore the current output mode
*/
static void outputModePush(ShellState *p) {
	p->modePrior = p->mode;
	memcpy(p->colSepPrior, p->colSeparator, sizeof(p->colSeparator));
	memcpy(p->rowSepPrior, p->rowSeparator, sizeof(p->rowSeparator));
}
static void outputModePop(ShellState *p) {
	p->mode = p->modePrior;
	memcpy(p->colSeparator, p->colSepPrior, sizeof(p->colSeparator));
	memcpy(p->rowSeparator, p->rowSepPrior, sizeof(p->rowSeparator));
}

/*
** Output the given string with characters that are special to
** HTML escaped.
*/
static void output_html_string(FILE *out, const char *z) {
	int i;
	if (z == 0)
		z = "";
	while (*z) {
		for (i = 0; z[i] && z[i] != '<' && z[i] != '&' && z[i] != '>' && z[i] != '\"' && z[i] != '\''; i++) {
		}
		if (i > 0) {
			utf8_printf(out, "%.*s", i, z);
		}
		if (z[i] == '<') {
			raw_printf(out, "&lt;");
		} else if (z[i] == '&') {
			raw_printf(out, "&amp;");
		} else if (z[i] == '>') {
			raw_printf(out, "&gt;");
		} else if (z[i] == '\"') {
			raw_printf(out, "&quot;");
		} else if (z[i] == '\'') {
			raw_printf(out, "&#39;");
		} else {
			break;
		}
		z += i + 1;
	}
}

/*
** If a field contains any character identified by a 1 in the following
** array, then the string must be quoted for CSV.
*/
static const char needCsvQuote[] = {
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0,
    0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
};

/*
** Output a single term of CSV.  Actually, p->colSeparator is used for
** the separator, which may or may not be a comma.  p->nullValue is
** the null value.  Strings are quoted if necessary.  The separator
** is only issued if bSep is true.
*/
static void output_csv(ShellState *p, const char *z, int bSep) {
	FILE *out = p->out;
	if (z == 0) {
		utf8_printf(out, "%s", p->nullValue);
	} else {
		int i;
		int nSep = strlen30(p->colSeparator);
		for (i = 0; z[i]; i++) {
			if (needCsvQuote[((unsigned char *)z)[i]] ||
			    (z[i] == p->colSeparator[0] && (nSep == 1 || memcmp(z, p->colSeparator, nSep) == 0))) {
				i = 0;
				break;
			}
		}
		if (i == 0) {
			char *zQuoted = sqlite3_mprintf("\"%w\"", z);
			utf8_printf(out, "%s", zQuoted);
			sqlite3_free(zQuoted);
		} else {
			utf8_printf(out, "%s", z);
		}
	}
	if (bSep) {
		utf8_printf(p->out, "%s", p->colSeparator);
	}
}

/*
** This routine runs when the user presses Ctrl-C
*/
static void interrupt_handler(int NotUsed) {
	UNUSED_PARAMETER(NotUsed);
	seenInterrupt++;
	if (seenInterrupt > 2)
		exit(1);
	if (globalDb)
		sqlite3_interrupt(globalDb);
}

/*
** This is the callback routine that the shell
** invokes for each row of a query result.
*/
static int shell_callback(void *pArg, int nArg, /* Number of result columns */
                          char **azArg,         /* Text of each result column */
                          char **azCol,         /* Column names */
                          int *aiType           /* Column types */
) {
	int i;
	ShellState *p = (ShellState *)pArg;

	if (azArg == 0)
		return 0;
	switch (p->cMode) {
	case MODE_Line: {
		int w = 5;
		if (azArg == 0)
			break;
		for (i = 0; i < nArg; i++) {
			int len = strlen30(azCol[i] ? azCol[i] : "");
			if (len > w)
				w = len;
		}
		if (p->cnt++ > 0)
			utf8_printf(p->out, "%s", p->rowSeparator);
		for (i = 0; i < nArg; i++) {
			utf8_printf(p->out, "%*s = %s%s", w, azCol[i], azArg[i] ? azArg[i] : p->nullValue, p->rowSeparator);
		}
		break;
	}
	case MODE_Explain:
	case MODE_Column: {
		static const int aExplainWidths[] = {4, 13, 4, 4, 4, 13, 2, 13};
		const int *colWidth;
		int showHdr;
		char *rowSep;
		if (p->cMode == MODE_Column) {
			colWidth = p->colWidth;
			showHdr = p->showHeader;
			rowSep = p->rowSeparator;
		} else {
			colWidth = aExplainWidths;
			showHdr = 1;
			rowSep = SEP_Row;
		}
		if (p->cnt++ == 0) {
			for (i = 0; i < nArg; i++) {
				int w, n;
				if (i < ArraySize(p->colWidth)) {
					w = colWidth[i];
				} else {
					w = 0;
				}
				if (w == 0) {
					w = strlenChar(azCol[i] ? azCol[i] : "");
					if (w < 10)
						w = 10;
					n = strlenChar(azArg && azArg[i] ? azArg[i] : p->nullValue);
					if (w < n)
						w = n;
				}
				if (i < ArraySize(p->actualWidth)) {
					p->actualWidth[i] = w;
				}
				if (showHdr) {
					utf8_width_print(p->out, w, azCol[i]);
					utf8_printf(p->out, "%s", i == nArg - 1 ? rowSep : "  ");
				}
			}
			if (showHdr) {
				for (i = 0; i < nArg; i++) {
					int w;
					if (i < ArraySize(p->actualWidth)) {
						w = p->actualWidth[i];
						if (w < 0)
							w = -w;
					} else {
						w = 10;
					}
					utf8_printf(p->out, "%-*.*s%s", w, w,
					            "----------------------------------------------"
					            "------------"
					            "----------------------------------------------"
					            "------------",
					            i == nArg - 1 ? rowSep : "  ");
				}
			}
		}
		if (azArg == 0)
			break;
		for (i = 0; i < nArg; i++) {
			int w;
			if (i < ArraySize(p->actualWidth)) {
				w = p->actualWidth[i];
			} else {
				w = 10;
			}
			if (p->cMode == MODE_Explain && azArg[i] && strlenChar(azArg[i]) > w) {
				w = strlenChar(azArg[i]);
			}
			if (i == 1 && p->aiIndent && p->pStmt) {
				if (p->iIndent < p->nIndent) {
					utf8_printf(p->out, "%*.s", p->aiIndent[p->iIndent], "");
				}
				p->iIndent++;
			}
			utf8_width_print(p->out, w, azArg[i] ? azArg[i] : p->nullValue);
			utf8_printf(p->out, "%s", i == nArg - 1 ? rowSep : "  ");
		}
		break;
	}
	case MODE_List: {
		if (p->cnt++ == 0 && p->showHeader) {
			for (i = 0; i < nArg; i++) {
				utf8_printf(p->out, "%s%s", azCol[i], i == nArg - 1 ? p->rowSeparator : p->colSeparator);
			}
		}
		if (azArg == 0)
			break;
		for (i = 0; i < nArg; i++) {
			char *z = azArg[i];
			if (z == 0)
				z = p->nullValue;
			utf8_printf(p->out, "%s", z);
			if (i < nArg - 1) {
				utf8_printf(p->out, "%s", p->colSeparator);
			} else {
				utf8_printf(p->out, "%s", p->rowSeparator);
			}
		}
		break;
	}
	case MODE_Html: {
		if (p->cnt++ == 0 && p->showHeader) {
			raw_printf(p->out, "<TR>");
			for (i = 0; i < nArg; i++) {
				raw_printf(p->out, "<TH>");
				output_html_string(p->out, azCol[i]);
				raw_printf(p->out, "</TH>\n");
			}
			raw_printf(p->out, "</TR>\n");
		}
		if (azArg == 0)
			break;
		raw_printf(p->out, "<TR>");
		for (i = 0; i < nArg; i++) {
			raw_printf(p->out, "<TD>");
			output_html_string(p->out, azArg[i] ? azArg[i] : p->nullValue);
			raw_printf(p->out, "</TD>\n");
		}
		raw_printf(p->out, "</TR>\n");
		break;
	}
	case MODE_Csv: {
		setBinaryMode(p->out, 1);
		if (p->cnt++ == 0 && p->showHeader) {
			for (i = 0; i < nArg; i++) {
				output_csv(p, azCol[i] ? azCol[i] : "", i < nArg - 1);
			}
			utf8_printf(p->out, "%s", p->rowSeparator);
		}
		if (nArg > 0) {
			for (i = 0; i < nArg; i++) {
				output_csv(p, azArg[i], i < nArg - 1);
			}
			utf8_printf(p->out, "%s", p->rowSeparator);
		}
		setTextMode(p->out, 1);
		break;
	}
	/*case MODE_Insert: {
		if( azArg==0 ) break;
		utf8_printf(p->out,"INSERT INTO %s",p->zDestTable);
		if( p->showHeader ){
		    raw_printf(p->out,"(");
		    for(i=0; i<nArg; i++){
		        if( i>0 ) raw_printf(p->out, ",");
		        if( quoteChar(azCol[i]) ){
		            char *z = sqlite3_mprintf("\"%w\"", azCol[i]);
		            utf8_printf(p->out, "%s", z);
		            sqlite3_free(z);
		        }else{
		            raw_printf(p->out, "%s", azCol[i]);
		        }
		    }
		    raw_printf(p->out,")");
		}
		p->cnt++;
		for(i=0; i<nArg; i++){
		    raw_printf(p->out, i>0 ? "," : " VALUES(");
		    if( (azArg[i]==0) || (aiType && aiType[i]==SQLITE_NULL) ){
		        utf8_printf(p->out,"NULL");
		    }else if( aiType && aiType[i]==SQLITE_TEXT ){
		        if( ShellHasFlag(p, SHFLG_Newlines) ){
		            output_quoted_string(p->out, azArg[i]);
		        }else{
		            output_quoted_escaped_string(p->out, azArg[i]);
		        }
		    }else if( aiType && aiType[i]==SQLITE_INTEGER ){
		        utf8_printf(p->out,"%s", azArg[i]);
		    }else if( aiType && aiType[i]==SQLITE_FLOAT ){
		        char z[50];
		        double r = sqlite3_column_double(p->pStmt, i);
		        sqlite3_snprintf(50,z,"%!.20g", r);
		        raw_printf(p->out, "%s", z);
		    }else if( aiType && aiType[i]==SQLITE_BLOB && p->pStmt ){
		        const void *pBlob = sqlite3_column_blob(p->pStmt, i);
		        int nBlob = sqlite3_column_bytes(p->pStmt, i);
		        output_hex_blob(p->out, pBlob, nBlob);
		    }else if( isNumber(azArg[i], 0) ){
		        utf8_printf(p->out,"%s", azArg[i]);
		    }else if( ShellHasFlag(p, SHFLG_Newlines) ){
		        output_quoted_string(p->out, azArg[i]);
		    }else{
		        output_quoted_escaped_string(p->out, azArg[i]);
		    }
		}
		raw_printf(p->out,");\n");
		break;
	}*/
	/*case MODE_Quote: {
		if( azArg==0 ) break;
		if( p->cnt==0 && p->showHeader ){
		    for(i=0; i<nArg; i++){
		        if( i>0 ) raw_printf(p->out, ",");
		        output_quoted_string(p->out, azCol[i]);
		    }
		    raw_printf(p->out,"\n");
		}
		p->cnt++;
		for(i=0; i<nArg; i++){
		    if( i>0 ) raw_printf(p->out, ",");
		    if( (azArg[i]==0) || (aiType && aiType[i]==SQLITE_NULL) ){
		        utf8_printf(p->out,"NULL");
		    }else if( aiType && aiType[i]==SQLITE_TEXT ){
		        output_quoted_string(p->out, azArg[i]);
		    }else if( aiType && aiType[i]==SQLITE_INTEGER ){
		        utf8_printf(p->out,"%s", azArg[i]);
		    }else if( aiType && aiType[i]==SQLITE_FLOAT ){
		        char z[50];
		        double r = sqlite3_column_double(p->pStmt, i);
		        sqlite3_snprintf(50,z,"%!.20g", r);
		        raw_printf(p->out, "%s", z);
		    }else if( aiType && aiType[i]==SQLITE_BLOB && p->pStmt ){
		        const void *pBlob = sqlite3_column_blob(p->pStmt, i);
		        int nBlob = sqlite3_column_bytes(p->pStmt, i);
		        output_hex_blob(p->out, pBlob, nBlob);
		    }else if( isNumber(azArg[i], 0) ){
		        utf8_printf(p->out,"%s", azArg[i]);
		    }else{
		        output_quoted_string(p->out, azArg[i]);
		    }
		}
		raw_printf(p->out,"\n");
		break;
	}*/
	case MODE_Ascii: {
		if (p->cnt++ == 0 && p->showHeader) {
			for (i = 0; i < nArg; i++) {
				if (i > 0)
					utf8_printf(p->out, "%s", p->colSeparator);
				utf8_printf(p->out, "%s", azCol[i] ? azCol[i] : "");
			}
			utf8_printf(p->out, "%s", p->rowSeparator);
		}
		if (azArg == 0)
			break;
		for (i = 0; i < nArg; i++) {
			if (i > 0)
				utf8_printf(p->out, "%s", p->colSeparator);
			utf8_printf(p->out, "%s", azArg[i] ? azArg[i] : p->nullValue);
		}
		utf8_printf(p->out, "%s", p->rowSeparator);
		break;
	}
	}
	return 0;
}

/*
** This is the callback routine that the SQLite library
** invokes for each row of a query result.
*/
static int callback(void *pArg, int nArg, char **azArg, char **azCol) {
	/* since we don't have type info, call the shell_callback with a NULL value
	 */
	return shell_callback(pArg, nArg, azArg, azCol, NULL);
}

/*
** Set the destination table field of the ShellState structure to
** the name of the table given.  Escape any quote characters in the
** table name.
*/
static void set_table_name(ShellState *p, const char *zName) {
	int i, n;
	char cQuote;
	char *z;

	if (p->zDestTable) {
		free(p->zDestTable);
		p->zDestTable = 0;
	}
	if (zName == 0)
		return;
	cQuote = quoteChar(zName);
	n = strlen30(zName);
	if (cQuote)
		n += n + 2;
	z = p->zDestTable = malloc(n + 1);
	if (z == 0) {
		raw_printf(stderr, "Error: out of memory\n");
		exit(1);
	}
	n = 0;
	if (cQuote)
		z[n++] = cQuote;
	for (i = 0; zName[i]; i++) {
		z[n++] = zName[i];
		if (zName[i] == cQuote)
			z[n++] = cQuote;
	}
	if (cQuote)
		z[n++] = cQuote;
	z[n] = 0;
}

/*
** Allocate space and save off current error string.
*/
static char *save_err_msg(sqlite3 *db /* Database to query */
) {
	int nErrMsg = 1 + strlen30(sqlite3_errmsg(db));
	char *zErrMsg = sqlite3_malloc64(nErrMsg);
	if (zErrMsg) {
		memcpy(zErrMsg, sqlite3_errmsg(db), nErrMsg);
	}
	return zErrMsg;
}

/*
** Run a prepared statement
*/
static int exec_prepared_stmt(ShellState *pArg,                                      /* Pointer to ShellState */
                               sqlite3_stmt *pStmt,                                   /* Statment to run */
                               int (*xCallback)(void *, int, char **, char **, int *) /* Callback function */
) {
	int rc;

	/* perform the first step.  this will tell us if we
	 ** have a result set or not and how wide it is.
	 */
	rc = sqlite3_step(pStmt);
	/* if we have a result set... */
	if (SQLITE_ROW == rc) {
		/* if we have a callback... */
		if (xCallback) {
			/* allocate space for col name ptr, value ptr, and type */
			int nCol = sqlite3_column_count(pStmt);
			void *pData = sqlite3_malloc64(3 * nCol * sizeof(const char *) + 1);
			if (!pData) {
				rc = SQLITE_NOMEM;
			} else {
				char **azCols = (char **)pData;      /* Names of result columns */
				char **azVals = &azCols[nCol];       /* Results */
				int *aiTypes = (int *)&azVals[nCol]; /* Result types */
				int i, x;
				assert(sizeof(int) <= sizeof(char *));
				/* save off ptrs to column names */
				for (i = 0; i < nCol; i++) {
					azCols[i] = (char *)sqlite3_column_name(pStmt, i);
				}
				do {
					/* extract the data and data types */
					for (i = 0; i < nCol; i++) {
						aiTypes[i] = x = sqlite3_column_type(pStmt, i);
						if (x == SQLITE_BLOB && pArg && pArg->cMode == MODE_Insert) {
							azVals[i] = "";
						} else {
							azVals[i] = (char *)sqlite3_column_text(pStmt, i);
						}
						if (!azVals[i] && (aiTypes[i] != SQLITE_NULL)) {
							rc = SQLITE_NOMEM;
							break; /* from for */
						}
					} /* end for */

					/* if data and types extracted successfully... */
					if (SQLITE_ROW == rc) {
						/* call the supplied callback with the result row data
						 */
						if (xCallback(pArg, nCol, azVals, azCols, aiTypes)) {
							rc = SQLITE_ABORT;
						} else {
							rc = sqlite3_step(pStmt);
						}
					}
				} while (SQLITE_ROW == rc);
				sqlite3_free(pData);
			}
		} else {
			do {
				rc = sqlite3_step(pStmt);
			} while (rc == SQLITE_ROW);
		}
	}
	return rc;
}

/*
** Execute a statement or set of statements.  Print
** any result rows/columns depending on the current mode
** set via the supplied callback.
**
** This is very similar to SQLite's built-in sqlite3_exec()
** function except it takes a slightly different callback
** and callback data argument.
*/
static int shell_exec(sqlite3 *db,                                            /* An open database */
                      const char *zSql,                                       /* SQL to be evaluated */
                      int (*xCallback)(void *, int, char **, char **, int *), /* Callback function */
                      /* (not the same as sqlite3_exec) */
                      ShellState *pArg, /* Pointer to ShellState */
                      char **pzErrMsg   /* Error msg written here */
) {
	sqlite3_stmt *pStmt = NULL; /* Statement to execute. */
	int rc = SQLITE_OK;         /* Return Code */
	int rc2;
	const char *zLeftover; /* Tail of unprocessed SQL */

	if (pzErrMsg) {
		*pzErrMsg = NULL;
	}

	while (zSql[0] && (SQLITE_OK == rc)) {
		static const char *zStmtSql;
		rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zLeftover);
		if (SQLITE_OK != rc) {
			if (pzErrMsg) {
				*pzErrMsg = save_err_msg(db);
			}
		} else {
			if (!pStmt) {
				/* this happens for a comment or white-space */
				zSql = zLeftover;
				while (IsSpace(zSql[0]))
					zSql++;
				continue;
			}
			zStmtSql = sqlite3_sql(pStmt);
			if (zStmtSql == 0)
				zStmtSql = "";
			while (IsSpace(zStmtSql[0]))
				zStmtSql++;

			/* save off the prepared statment handle and reset row count */
			if (pArg) {
				pArg->pStmt = pStmt;
				pArg->cnt = 0;
			}

			/* echo the sql statement if echo on */
			if (pArg && ShellHasFlag(pArg, SHFLG_Echo)) {
				utf8_printf(pArg->out, "%s\n", zStmtSql ? zStmtSql : zSql);
			}

			rc = exec_prepared_stmt(pArg, pStmt, xCallback);
			if (rc == SQLITE_ERROR) {
				if (pzErrMsg) {
					*pzErrMsg = save_err_msg(db);
				}
			}

			/* Finalize the statement just executed. If this fails, save a
			 ** copy of the error message. Otherwise, set zSql to point to the
			 ** next statement to execute. */
			rc2 = sqlite3_finalize(pStmt);
			if (rc != SQLITE_NOMEM)
				rc = rc2;
			if (rc == SQLITE_OK) {
				zSql = zLeftover;
				while (IsSpace(zSql[0]))
					zSql++;
			} else if (pzErrMsg) {
				*pzErrMsg = save_err_msg(db);
			}

			/* clear saved stmt handle */
			if (pArg) {
				pArg->pStmt = NULL;
			}
		}
	} /* end while */

	return rc;
}

/*
** Text of a help message
*/
static char zHelp[] = ".cd DIRECTORY          Change the working directory to DIRECTORY\n"
                      ".echo on|off           Turn command echo on or off\n"
                      ".headers on|off        Turn display of headers on or off\n"
                      ".help                  Show this message\n"
                      ".mode MODE ?TABLE?     Set output mode where MODE is one of:\n"
                      "                         ascii    Columns/rows delimited by 0x1F and "
                      "0x1E\n"
                      "                         csv      Comma-separated values\n"
                      "                         column   Left-aligned columns.  (See .width)\n"
                      "                         html     HTML <table> code\n"
                      "                         line     One value per line\n"
                      "                         list     Values delimited by \"|\"\n"
                      "                         tabs     Tab-separated values\n"
                      ".once (-e|-x|FILE)     Output for the next SQL command only to FILE\n"
                      "     Other options:\n"
                      "       -e    Invoke system text editor\n"
                      "       -x    Invoke system csv editor\n"
                      ".output ?FILE?         Send output to FILE or stdout if FILE is omitted\n"
                      ".print STRING...       Print literal STRING\n"
                      ".excel                 Display the output of next command in the system csv viewer\n"
                      ".exit                  Exit this program\n"
                      ".quit                  Exit this program\n"
                      ".read FILENAME         Execute SQL in FILENAME\n"
                      ".separator COL ?ROW?   Change the column separator and optionally the "
                      "row\n"
                      "                         separator for both the output mode and .import\n"
                      ".width NUM1 NUM2 ...   Set column widths for \"column\" mode\n"
                      "                         Negative values right-justify\n";

/* Forward reference */
static int process_input(ShellState *p, FILE *in);

/*
** Make sure the database is open.  If it is not, then open it.  If
** the database fails to open, print an error message and exit.
*/
static void open_db(ShellState *p, int keepAlive) {
	if (p->db == 0) {
		sqlite3_initialize();
		switch (p->openMode) {
		case SHELL_OPEN_UNSPEC:
		case SHELL_OPEN_NORMAL: {
			sqlite3_open(p->zDbFilename, &p->db);
			break;
		}
		}
		globalDb = p->db;
		if (p->db == 0 || SQLITE_OK != sqlite3_errcode(p->db)) {
			utf8_printf(stderr, "Error: unable to open database \"%s\": %s\n", p->zDbFilename, sqlite3_errmsg(p->db));
			if (keepAlive)
				return;
			exit(1);
		}
	}
}

#if HAVE_READLINE || HAVE_EDITLINE
/*
** Readline completion callbacks
*/

static char *readline_completion_generator(const char *text, int state) {
	static int current_keyword_index, text_len;
	const size_t n_keywords = sizeof(azKeywords) / sizeof(azKeywords[0]);
	const char *name;

	if (!state) {
		current_keyword_index = 0;
		text_len = (int)strlen(text);
	}

	while (current_keyword_index < n_keywords) {
		name = azKeywords[current_keyword_index++];
		if (sqlite3_strnicmp(name, text, text_len) == 0) {
			return strdup(name);
		}
	}

	return NULL;
}

static char **readline_completion(const char *zText, int iStart, int iEnd) {
	rl_attempted_completion_over = 1;
	return rl_completion_matches(zText, readline_completion_generator);
}

#elif HAVE_LINENOISE
/*
** Linenoise completion callback
*/
static void linenoise_completion(const char *zLine, linenoiseCompletions *lc) {
	int nLine = strlen30(zLine);
	int i, iStart;
	sqlite3_stmt *pStmt = 0;
	char *zSql;
	char zBuf[1000];

	if (nLine > sizeof(zBuf) - 30)
		return;
	if (zLine[0] == '.')
		return;
	for (i = nLine - 1; i >= 0 && (isalnum(zLine[i]) || zLine[i] == '_'); i--) {
	}
	if (i == nLine - 1)
		return;
	iStart = i + 1;
	memcpy(zBuf, zLine, iStart);
	zSql = sqlite3_mprintf("SELECT DISTINCT candidate COLLATE nocase"
	                       "  FROM completion(%Q,%Q) ORDER BY 1",
	                       &zLine[iStart], zLine);
	sqlite3_prepare_v2(globalDb, zSql, -1, &pStmt, 0);
	sqlite3_free(zSql);
	sqlite3_exec(globalDb, "PRAGMA page_count", 0, 0, 0); /* Load the schema */
	while (sqlite3_step(pStmt) == SQLITE_ROW) {
		const char *zCompletion = (const char *)sqlite3_column_text(pStmt, 0);
		int nCompletion = sqlite3_column_bytes(pStmt, 0);
		if (iStart + nCompletion < sizeof(zBuf) - 1) {
			memcpy(zBuf + iStart, zCompletion, nCompletion + 1);
			linenoiseAddCompletion(lc, zBuf);
		}
	}
	sqlite3_finalize(pStmt);
}
#endif

/*
** Do C-language style dequoting.
**
**    \a    -> alarm
**    \b    -> backspace
**    \t    -> tab
**    \n    -> newline
**    \v    -> vertical tab
**    \f    -> form feed
**    \r    -> carriage return
**    \s    -> space
**    \"    -> "
**    \'    -> '
**    \\    -> backslash
**    \NNN  -> ascii character NNN in octal
*/
static void resolve_backslashes(char *z) {
	int i, j;
	char c;
	while (*z && *z != '\\')
		z++;
	for (i = j = 0; (c = z[i]) != 0; i++, j++) {
		if (c == '\\' && z[i + 1] != 0) {
			c = z[++i];
			if (c == 'a') {
				c = '\a';
			} else if (c == 'b') {
				c = '\b';
			} else if (c == 't') {
				c = '\t';
			} else if (c == 'n') {
				c = '\n';
			} else if (c == 'v') {
				c = '\v';
			} else if (c == 'f') {
				c = '\f';
			} else if (c == 'r') {
				c = '\r';
			} else if (c == '"') {
				c = '"';
			} else if (c == '\'') {
				c = '\'';
			} else if (c == '\\') {
				c = '\\';
			} else if (c >= '0' && c <= '7') {
				c -= '0';
				if (z[i + 1] >= '0' && z[i + 1] <= '7') {
					i++;
					c = (c << 3) + z[i] - '0';
					if (z[i + 1] >= '0' && z[i + 1] <= '7') {
						i++;
						c = (c << 3) + z[i] - '0';
					}
				}
			}
		}
		z[j] = c;
	}
	if (j < i)
		z[j] = 0;
}

/*
** Interpret zArg as either an integer or a boolean value.  Return 1 or 0
** for TRUE and FALSE.  Return the integer value if appropriate.
*/
static int booleanValue(const char *zArg) {
	int i;
	if (zArg[0] == '0' && zArg[1] == 'x') {
		for (i = 2; hexDigitValue(zArg[i]) >= 0; i++) {
		}
	} else {
		for (i = 0; zArg[i] >= '0' && zArg[i] <= '9'; i++) {
		}
	}
	if (i > 0 && zArg[i] == 0)
		return (int)(integerValue(zArg) & 0xffffffff);
	if (sqlite3_stricmp(zArg, "on") == 0 || sqlite3_stricmp(zArg, "yes") == 0) {
		return 1;
	}
	if (sqlite3_stricmp(zArg, "off") == 0 || sqlite3_stricmp(zArg, "no") == 0) {
		return 0;
	}
	utf8_printf(stderr, "ERROR: Not a boolean value: \"%s\". Assuming \"no\".\n", zArg);
	return 0;
}

/*
** Set or clear a shell flag according to a boolean value.
*/
static void setOrClearFlag(ShellState *p, unsigned mFlag, const char *zArg) {
	if (booleanValue(zArg)) {
		ShellSetFlag(p, mFlag);
	} else {
		ShellClearFlag(p, mFlag);
	}
}

/*
** Close an output file, assuming it is not stderr or stdout
*/
static void output_file_close(FILE *f) {
	if (f && f != stdout && f != stderr)
		fclose(f);
}

/*
** Try to open an output file.   The names "stdout" and "stderr" are
** recognized and do the right thing.  NULL is returned if the output
** filename is "off".
*/
static FILE *output_file_open(const char *zFile, int bTextMode) {
	FILE *f;
	if (strcmp(zFile, "stdout") == 0) {
		f = stdout;
	} else if (strcmp(zFile, "stderr") == 0) {
		f = stderr;
	} else if (strcmp(zFile, "off") == 0) {
		f = 0;
	} else {
		f = fopen(zFile, bTextMode ? "w" : "wb");
		if (f == 0) {
			utf8_printf(stderr, "Error: cannot open \"%s\"\n", zFile);
		}
	}
	return f;
}

/*
** An object used to read a CSV and other files for import.
*/
typedef struct ImportCtx ImportCtx;
struct ImportCtx {
	const char *zFile; /* Name of the input file */
	FILE *in;          /* Read the CSV text from this input stream */
	char *z;           /* Accumulated text for a field */
	int n;             /* Number of bytes in z */
	int nAlloc;        /* Space allocated for z[] */
	int nLine;         /* Current line number */
	int bNotFirst;     /* True if one or more bytes already read */
	int cTerm;         /* Character that terminated the most recent field */
	int cColSep;       /* The column separator character.  (Usually ",") */
	int cRowSep;       /* The row separator character.  (Usually "\n") */
};

/*
** Change the output file back to stdout.
**
** If the p->doXdgOpen flag is set, that means the output was being
** redirected to a temporary file named by p->zTempFile.  In that case,
** launch start/open/xdg-open on that temporary file.
*/
static void output_reset(ShellState *p) {
	if (p->outfile[0] == '|') {
		pclose(p->out);
	} else {
		output_file_close(p->out);
		// #ifndef SQLITE_NOHAVE_SYSTEM
		if (p->doXdgOpen) {
			const char *zXdgOpenCmd =
#if defined(_WIN32)
			    "start";
#elif defined(__APPLE__)
			    "open";
#else
			    "xdg-open";
#endif
			char *zCmd;
			zCmd = sqlite3_mprintf("%s %s", zXdgOpenCmd, p->zTempFile);
			if (system(zCmd)) {
				utf8_printf(stderr, "Failed: [%s]\n", zCmd);
			}
			sqlite3_free(zCmd);
			outputModePop(p);
			p->doXdgOpen = 0;
		}
		// #endif
	}
	p->outfile[0] = 0;
	p->out = stdout;
}

/*
** Compare the string as a command-line option with either one or two
** initial "-" characters.
*/
static int optionMatch(const char *zStr, const char *zOpt) {
	if (zStr[0] != '-')
		return 0;
	zStr++;
	if (zStr[0] == '-')
		zStr++;
	return strcmp(zStr, zOpt) == 0;
}

/*
** Delete a file.
*/
int shellDeleteFile(const char *zFilename) {
	int rc;
	rc = unlink(zFilename);
	return rc;
}

/*
** Try to delete the temporary file (if there is one) and free the
** memory used to hold the name of the temp file.
*/
static void clearTempFile(ShellState *p) {
	if (p->zTempFile == 0)
		return;
	if (p->doXdgOpen)
		return;
	if (shellDeleteFile(p->zTempFile))
		return;
	sqlite3_free(p->zTempFile);
	p->zTempFile = 0;
}

/*
** Create a new temp file name with the given suffix.
*/
static void newTempFile(ShellState *p, const char *zSuffix) {
	clearTempFile(p);
	sqlite3_free(p->zTempFile);
	p->zTempFile = 0;
	if (p->zTempFile == 0) {
		sqlite3_uint64 r;
		sqlite3_randomness(sizeof(r), &r);
		p->zTempFile = sqlite3_mprintf("temp%llx.%s", r, zSuffix);
	} else {
		p->zTempFile = sqlite3_mprintf("%z.%s", p->zTempFile, zSuffix);
	}
	if (p->zTempFile == 0) {
		raw_printf(stderr, "out of memory\n");
		exit(1);
	}
}

/*
** If an input line begins with "." then invoke this routine to
** process that line.
**
** Return 1 on error, 2 to exit, and 0 otherwise.
*/
static int do_meta_command(char *zLine, ShellState *p) {
	int h = 1;
	int nArg = 0;
	int n, c;
	int rc = 0;
	char *azArg[50];

	/* Parse the input line into tokens.
	 */
	while (zLine[h] && nArg < ArraySize(azArg)) {
		while (IsSpace(zLine[h])) {
			h++;
		}
		if (zLine[h] == 0)
			break;
		if (zLine[h] == '\'' || zLine[h] == '"') {
			int delim = zLine[h++];
			azArg[nArg++] = &zLine[h];
			while (zLine[h] && zLine[h] != delim) {
				if (zLine[h] == '\\' && delim == '"' && zLine[h + 1] != 0)
					h++;
				h++;
			}
			if (zLine[h] == delim) {
				zLine[h++] = 0;
			}
			if (delim == '"')
				resolve_backslashes(azArg[nArg - 1]);
		} else {
			azArg[nArg++] = &zLine[h];
			while (zLine[h] && !IsSpace(zLine[h])) {
				h++;
			}
			if (zLine[h])
				zLine[h++] = 0;
			resolve_backslashes(azArg[nArg - 1]);
		}
	}

	/* Process the input line.
	 */
	if (nArg == 0)
		return 0; /* no tokens, no error */
	n = strlen30(azArg[0]);
	c = azArg[0][0];
	clearTempFile(p);

	if (c == 'b' && n >= 3 && strncmp(azArg[0], "bail", n) == 0) {
		if (nArg == 2) {
			bail_on_error = booleanValue(azArg[1]);
		} else {
			raw_printf(stderr, "Usage: .bail on|off\n");
			rc = 1;
		}
	} else

	    if (c == 'c' && strcmp(azArg[0], "cd") == 0) {
		if (nArg == 2) {
			rc = chdir(azArg[1]);
			if (rc) {
				utf8_printf(stderr, "Cannot change to directory \"%s\"\n", azArg[1]);
				rc = 1;
			}
		} else {
			raw_printf(stderr, "Usage: .cd DIRECTORY\n");
			rc = 1;
		}
	} else

	    if (c == 'd' && n > 1 && strncmp(azArg[0], "databases", n) == 0) {
		ShellState data;
		char *zErrMsg = 0;
		open_db(p, 0);
		memcpy(&data, p, sizeof(data));
		data.showHeader = 0;
		data.cMode = data.mode = MODE_List;
		sqlite3_snprintf(sizeof(data.colSeparator), data.colSeparator, ": ");
		data.cnt = 0;
		sqlite3_exec(p->db, "SELECT name, file FROM pragma_database_list", callback, &data, &zErrMsg);
		if (zErrMsg) {
			utf8_printf(stderr, "Error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
			rc = 1;
		}
	} else

	    if (c == 'e' && strncmp(azArg[0], "echo", n) == 0) {
		if (nArg == 2) {
			setOrClearFlag(p, SHFLG_Echo, azArg[1]);
		} else {
			raw_printf(stderr, "Usage: .echo on|off\n");
			rc = 1;
		}
	} else

	    if (c == 'e' && strncmp(azArg[0], "exit", n) == 0) {
		if (nArg > 1 && (rc = (int)integerValue(azArg[1])) != 0)
			exit(rc);
		rc = 2;
	} else

	    if (c == 'h' && strncmp(azArg[0], "headers", n) == 0) {
		if (nArg == 2) {
			p->showHeader = booleanValue(azArg[1]);
		} else {
			raw_printf(stderr, "Usage: .headers on|off\n");
			rc = 1;
		}
	} else

	    if (c == 'h' && strncmp(azArg[0], "help", n) == 0) {
		utf8_printf(p->out, "%s", zHelp);
	} else

	    //    if( c=='i' && strncmp(azArg[0], "import", n)==0 ){
	    //        char *zTable;               /* Insert data into this table */
	    //        char *zFile;                /* Name of file to extra content
	    //        from */ sqlite3_stmt *pStmt = NULL; /* A statement */ int
	    //        nCol;                   /* Number of columns in the table */
	    //        int nByte;                  /* Number of bytes in an SQL
	    //        string */ int i, j;                   /* Loop counters */ int
	    //        needCommit;             /* True to COMMIT or ROLLBACK at end
	    //        */ int nSep;                   /* Number of bytes in
	    //        p->colSeparator[] */ char *zSql;                 /* An SQL
	    //        statement */ ImportCtx sCtx;             /* Reader context */
	    //        char *(SQLITE_CDECL *xRead)(ImportCtx*); /* Func to read one
	    //        value */ int (SQLITE_CDECL *xCloser)(FILE*);      /* Func to
	    //        close file */
	    //
	    //        if( nArg!=3 ){
	    //            raw_printf(stderr, "Usage: .import FILE TABLE\n");
	    //            goto meta_command_exit;
	    //        }
	    //        zFile = azArg[1];
	    //        zTable = azArg[2];
	    //        seenInterrupt = 0;
	    //        memset(&sCtx, 0, sizeof(sCtx));
	    //        open_db(p, 0);
	    //        nSep = strlen30(p->colSeparator);
	    //        if( nSep==0 ){
	    //            raw_printf(stderr,
	    //                       "Error: non-null column separator required for
	    //                       import\n");
	    //            return 1;
	    //        }
	    //        if( nSep>1 ){
	    //            raw_printf(stderr, "Error: multi-character column
	    //            separators not allowed"
	    //                    " for import\n");
	    //            return 1;
	    //        }
	    //        nSep = strlen30(p->rowSeparator);
	    //        if( nSep==0 ){
	    //            raw_printf(stderr, "Error: non-null row separator required
	    //            for import\n"); return 1;
	    //        }
	    //        if( nSep==2 && p->mode==MODE_Csv && strcmp(p->rowSeparator,
	    //        SEP_CrLf)==0 ){
	    //            /* When importing CSV (only), if the row separator is set
	    //            to the
	    //      ** default output row separator, change it to the default input
	    //      ** row separator.  This avoids having to maintain different
	    //      input
	    //      ** and output row separators. */
	    //            sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator,
	    //            SEP_Row); nSep = strlen30(p->rowSeparator);
	    //        }
	    //        if( nSep>1 ){
	    //            raw_printf(stderr, "Error: multi-character row separators
	    //            not allowed"
	    //                    " for import\n");
	    //            return 1;
	    //        }
	    //        sCtx.zFile = zFile;
	    //        sCtx.nLine = 1;
	    //        if( sCtx.zFile[0]=='|' ){
	    //            sCtx.in = popen(sCtx.zFile+1, "r");
	    //            sCtx.zFile = "<pipe>";
	    //            xCloser = pclose;
	    //        }else{
	    //            sCtx.in = fopen(sCtx.zFile, "rb");
	    //            xCloser = fclose;
	    //        }
	    //        if( p->mode==MODE_Ascii ){
	    //            xRead = ascii_read_one_field;
	    //        }else{
	    //            xRead = csv_read_one_field;
	    //        }
	    //        if( sCtx.in==0 ){
	    //            utf8_printf(stderr, "Error: cannot open \"%s\"\n", zFile);
	    //            return 1;
	    //        }
	    //        sCtx.cColSep = p->colSeparator[0];
	    //        sCtx.cRowSep = p->rowSeparator[0];
	    //        zSql = sqlite3_mprintf("SELECT * FROM %s", zTable);
	    //        if( zSql==0 ){
	    //            raw_printf(stderr, "Error: out of memory\n");
	    //            xCloser(sCtx.in);
	    //            return 1;
	    //        }
	    //        nByte = strlen30(zSql);
	    //        rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
	    //        import_append_char(&sCtx, 0);    /* To ensure sCtx.z is
	    //        allocated */ if( rc && sqlite3_strglob("no such table: *",
	    //        sqlite3_errmsg(p->db))==0 ){
	    //            char *zCreate = sqlite3_mprintf("CREATE TABLE %s",
	    //            zTable); char cSep = '('; while( xRead(&sCtx) ){
	    //                zCreate = sqlite3_mprintf("%z%c\n  \"%w\" TEXT",
	    //                zCreate, cSep, sCtx.z); cSep = ','; if(
	    //                sCtx.cTerm!=sCtx.cColSep ) break;
	    //            }
	    //            if( cSep=='(' ){
	    //                sqlite3_free(zCreate);
	    //                sqlite3_free(sCtx.z);
	    //                xCloser(sCtx.in);
	    //                utf8_printf(stderr,"%s: empty file\n", sCtx.zFile);
	    //                return 1;
	    //            }
	    //            zCreate = sqlite3_mprintf("%z\n)", zCreate);
	    //            rc = sqlite3_exec(p->db, zCreate, 0, 0, 0);
	    //            sqlite3_free(zCreate);
	    //            if( rc ){
	    //                utf8_printf(stderr, "CREATE TABLE %s(...) failed:
	    //                %s\n", zTable,
	    //                            sqlite3_errmsg(p->db));
	    //                sqlite3_free(sCtx.z);
	    //                xCloser(sCtx.in);
	    //                return 1;
	    //            }
	    //            rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
	    //        }
	    //        sqlite3_free(zSql);
	    //        if( rc ){
	    //            if (pStmt) sqlite3_finalize(pStmt);
	    //            utf8_printf(stderr,"Error: %s\n", sqlite3_errmsg(p->db));
	    //            xCloser(sCtx.in);
	    //            return 1;
	    //        }
	    //        nCol = sqlite3_column_count(pStmt);
	    //        sqlite3_finalize(pStmt);
	    //        pStmt = 0;
	    //        if( nCol==0 ) return 0; /* no columns, no error */
	    //        zSql = sqlite3_malloc64( nByte*2 + 20 + nCol*2 );
	    //        if( zSql==0 ){
	    //            raw_printf(stderr, "Error: out of memory\n");
	    //            xCloser(sCtx.in);
	    //            return 1;
	    //        }
	    //        sqlite3_snprintf(nByte+20, zSql, "INSERT INTO \"%w\"
	    //        VALUES(?", zTable); j = strlen30(zSql); for(i=1; i<nCol; i++){
	    //            zSql[j++] = ',';
	    //            zSql[j++] = '?';
	    //        }
	    //        zSql[j++] = ')';
	    //        zSql[j] = 0;
	    //        rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
	    //        sqlite3_free(zSql);
	    //        if( rc ){
	    //            utf8_printf(stderr, "Error: %s\n", sqlite3_errmsg(p->db));
	    //            if (pStmt) sqlite3_finalize(pStmt);
	    //            xCloser(sCtx.in);
	    //            return 1;
	    //        }
	    //        needCommit = sqlite3_get_autocommit(p->db);
	    //        if( needCommit ) sqlite3_exec(p->db, "BEGIN", 0, 0, 0);
	    //        do{
	    //            int startLine = sCtx.nLine;
	    //            for(i=0; i<nCol; i++){
	    //                char *z = xRead(&sCtx);
	    //                /*
	    //        ** Did we reach end-of-file before finding any columns?
	    //        ** If so, stop instead of NULL filling the remaining columns.
	    //        */
	    //                if( z==0 && i==0 ) break;
	    //                /*
	    //        ** Did we reach end-of-file OR end-of-line before finding any
	    //        ** columns in ASCII mode?  If so, stop instead of NULL filling
	    //        ** the remaining columns.
	    //        */
	    //                if( p->mode==MODE_Ascii && (z==0 || z[0]==0) && i==0 )
	    //                break; sqlite3_bind_text(pStmt, i+1, z, -1,
	    //                SQLITE_TRANSIENT); if( i<nCol-1 &&
	    //                sCtx.cTerm!=sCtx.cColSep ){
	    //                    utf8_printf(stderr, "%s:%d: expected %d columns
	    //                    but found %d - "
	    //                                        "filling the rest with
	    //                                        NULL\n",
	    //                                sCtx.zFile, startLine, nCol, i+1);
	    //                    i += 2;
	    //                    while( i<=nCol ){ sqlite3_bind_null(pStmt, i);
	    //                    i++; }
	    //                }
	    //            }
	    //            if( sCtx.cTerm==sCtx.cColSep ){
	    //                do{
	    //                    xRead(&sCtx);
	    //                    i++;
	    //                }while( sCtx.cTerm==sCtx.cColSep );
	    //                utf8_printf(stderr, "%s:%d: expected %d columns but
	    //                found %d - "
	    //                                    "extras ignored\n",
	    //                            sCtx.zFile, startLine, nCol, i);
	    //            }
	    //            if( i>=nCol ){
	    //                sqlite3_step(pStmt);
	    //                rc = sqlite3_reset(pStmt);
	    //                if( rc!=SQLITE_OK ){
	    //                    utf8_printf(stderr, "%s:%d: INSERT failed: %s\n",
	    //                    sCtx.zFile,
	    //                                startLine, sqlite3_errmsg(p->db));
	    //                }
	    //            }
	    //        }while( sCtx.cTerm!=EOF );
	    //
	    //        xCloser(sCtx.in);
	    //        sqlite3_free(sCtx.z);
	    //        sqlite3_finalize(pStmt);
	    //        if( needCommit ) sqlite3_exec(p->db, "COMMIT", 0, 0, 0);
	    //    }else

	    if (c == 'l' && strncmp(azArg[0], "log", n) == 0) {
		if (nArg != 2) {
			raw_printf(stderr, "Usage: .log FILENAME\n");
			rc = 1;
		} else {
			const char *zFile = azArg[1];
			output_file_close(p->pLog);
			p->pLog = output_file_open(zFile, 0);
		}
	} else

	    if (c == 'm' && strncmp(azArg[0], "mode", n) == 0) {
		const char *zMode = nArg >= 2 ? azArg[1] : "";
		int n2 = strlen30(zMode);
		int c2 = zMode[0];
		if (c2 == 'l' && n2 > 2 && strncmp(azArg[1], "lines", n2) == 0) {
			p->mode = MODE_Line;
			sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator, SEP_Row);
		} else if (c2 == 'c' && strncmp(azArg[1], "columns", n2) == 0) {
			p->mode = MODE_Column;
			sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator, SEP_Row);
		} else if (c2 == 'l' && n2 > 2 && strncmp(azArg[1], "list", n2) == 0) {
			p->mode = MODE_List;
			sqlite3_snprintf(sizeof(p->colSeparator), p->colSeparator, SEP_Column);
			sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator, SEP_Row);
		} else if (c2 == 'h' && strncmp(azArg[1], "html", n2) == 0) {
			p->mode = MODE_Html;
		} else if (c2 == 'c' && strncmp(azArg[1], "csv", n2) == 0) {
			p->mode = MODE_Csv;
			sqlite3_snprintf(sizeof(p->colSeparator), p->colSeparator, SEP_Comma);
			sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator, SEP_CrLf);
		} else if (c2 == 't' && strncmp(azArg[1], "tabs", n2) == 0) {
			p->mode = MODE_List;
			sqlite3_snprintf(sizeof(p->colSeparator), p->colSeparator, SEP_Tab);
		} else if (c2 == 'i' && strncmp(azArg[1], "insert", n2) == 0) {
			p->mode = MODE_Insert;
			set_table_name(p, nArg >= 3 ? azArg[2] : "table");
		} else if (c2 == 'q' && strncmp(azArg[1], "quote", n2) == 0) {
			p->mode = MODE_Quote;
		} else if (c2 == 'a' && strncmp(azArg[1], "ascii", n2) == 0) {
			p->mode = MODE_Ascii;
			sqlite3_snprintf(sizeof(p->colSeparator), p->colSeparator, SEP_Unit);
			sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator, SEP_Record);
		} else if (nArg == 1) {
			raw_printf(p->out, "current output mode: %s\n", modeDescr[p->mode]);
		} else {
			raw_printf(stderr, "Error: mode should be one of: "
			                   "ascii column csv html insert line list quote tabs tcl\n");
			rc = 1;
		}
		p->cMode = p->mode;
	} else

	    if (c == 'n' && strncmp(azArg[0], "nullvalue", n) == 0) {
		if (nArg == 2) {
			sqlite3_snprintf(sizeof(p->nullValue), p->nullValue, "%.*s", (int)ArraySize(p->nullValue) - 1, azArg[1]);
		} else {
			raw_printf(stderr, "Usage: .nullvalue STRING\n");
			rc = 1;
		}
	} else

	    if (c == 'o' && strncmp(azArg[0], "open", n) == 0 && n >= 2) {
		char *zNewFilename; /* Name of the database file to open */
		int iName = 1;      /* Index in azArg[] of the filename */
		int newFlag = 0;    /* True to delete file before opening */
		/* Close the existing database */
		sqlite3_close(p->db);
		p->db = 0;
		p->zDbFilename = 0;
		sqlite3_free(p->zFreeOnClose);
		p->zFreeOnClose = 0;
		p->openMode = SHELL_OPEN_UNSPEC;
		/* Check for command-line arguments */
		for (iName = 1; iName < nArg && azArg[iName][0] == '-'; iName++) {
			const char *z = azArg[iName];
			if (optionMatch(z, "new")) {
				newFlag = 1;
			} else if (z[0] == '-') {
				utf8_printf(stderr, "unknown option: %s\n", z);
				rc = 1;
				goto meta_command_exit;
			}
		}
		/* If a filename is specified, try to open it first */
		zNewFilename = nArg > iName ? sqlite3_mprintf("%s", azArg[iName]) : 0;
		if (zNewFilename) {
			if (newFlag)
				shellDeleteFile(zNewFilename);
			p->zDbFilename = zNewFilename;
			open_db(p, 1);
			if (p->db == 0) {
				utf8_printf(stderr, "Error: cannot open '%s'\n", zNewFilename);
				sqlite3_free(zNewFilename);
			} else {
				p->zFreeOnClose = zNewFilename;
			}
		}
		if (p->db == 0) {
			/* As a fall-back open a TEMP database */
			p->zDbFilename = 0;
			open_db(p, 0);
		}
	} else

	    if ((c == 'o' && (strncmp(azArg[0], "output", n) == 0 || strncmp(azArg[0], "once", n) == 0)) ||
	        (c == 'e' && n == 5 && strcmp(azArg[0], "excel") == 0)) {
		const char *zFile = nArg >= 2 ? azArg[1] : "stdout";
		int bTxtMode = 0;
		if (azArg[0][0] == 'e') {

			nArg = 2;
			azArg[0] = "once";
			zFile = azArg[1] = "-x";
			n = 4;
		}
		if (nArg > 2) {
			utf8_printf(stderr, "Usage: .%s [-e|-x|FILE]\n", azArg[0]);
			rc = 1;
			goto meta_command_exit;
		}
		if (n > 1 && strncmp(azArg[0], "once", n) == 0) {
			if (nArg < 2) {
				raw_printf(stderr, "Usage: .once (-e|-x|FILE)\n");
				rc = 1;
				goto meta_command_exit;
			}
			p->outCount = 2;
		} else {
			p->outCount = 0;
		}
		output_reset(p);
		if (zFile[0] == '-' && zFile[1] == '-')
			zFile++;
		// #ifndef SQLITE_NOHAVE_SYSTEM
		if (strcmp(zFile, "-e") == 0 || strcmp(zFile, "-x") == 0) {
			p->doXdgOpen = 1;
			outputModePush(p);
			if (zFile[1] == 'x') {
				newTempFile(p, "csv");
				p->mode = MODE_Csv;
				sqlite3_snprintf(sizeof(p->colSeparator), p->colSeparator, SEP_Comma);
				sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator, SEP_CrLf);
			} else {
				newTempFile(p, "txt");
				bTxtMode = 1;
			}
			zFile = p->zTempFile;
		}
		// #endif
		//         if( zFile[0]=='|' ){
		//             p->out = popen(zFile + 1, "w");
		//             if( p->out==0 ){
		//                 utf8_printf(stderr,"Error: cannot open pipe \"%s\"\n",
		// zFile + 1); p->out = stdout; rc = 1; }else{
		//                 sqlite3_snprintf(sizeof(p->outfile), p->outfile, "%s",
		// zFile);
		//             }
		//         }else{
		p->out = output_file_open(zFile, bTxtMode);
		if (p->out == 0) {
			if (strcmp(zFile, "off") != 0) {
				utf8_printf(stderr, "Error: cannot write to \"%s\"\n", zFile);
			}
			p->out = stdout;
			rc = 1;
		} else {
			sqlite3_snprintf(sizeof(p->outfile), p->outfile, "%s", zFile);
		}
		// }
	} else

	    if (c == 'p' && n >= 3 && strncmp(azArg[0], "print", n) == 0) {
		int i;
		for (i = 1; i < nArg; i++) {
			if (i > 1)
				raw_printf(p->out, " ");
			utf8_printf(p->out, "%s", azArg[i]);
		}
		raw_printf(p->out, "\n");
	} else

	    if (c == 'p' && strncmp(azArg[0], "prompt", n) == 0) {
		if (nArg >= 2) {
			strncpy(mainPrompt, azArg[1], (int)ArraySize(mainPrompt) - 1);
		}
		if (nArg >= 3) {
			strncpy(continuePrompt, azArg[2], (int)ArraySize(continuePrompt) - 1);
		}
	} else

	    if (c == 'q' && strncmp(azArg[0], "quit", n) == 0) {
		rc = 2;
	} else

	    if (c == 'r' && n >= 3 && strncmp(azArg[0], "read", n) == 0) {
		FILE *alt;
		if (nArg != 2) {
			raw_printf(stderr, "Usage: .read FILE\n");
			rc = 1;
			goto meta_command_exit;
		}
		alt = fopen(azArg[1], "rb");
		if (alt == 0) {
			utf8_printf(stderr, "Error: cannot open \"%s\"\n", azArg[1]);
			rc = 1;
		} else {
			rc = process_input(p, alt);
			fclose(alt);
		}
	} else

	    if (c == 's' && strncmp(azArg[0], "separator", n) == 0) {
		if (nArg < 2 || nArg > 3) {
			raw_printf(stderr, "Usage: .separator COL ?ROW?\n");
			rc = 1;
		}
		if (nArg >= 2) {
			sqlite3_snprintf(sizeof(p->colSeparator), p->colSeparator, "%.*s", (int)ArraySize(p->colSeparator) - 1,
			                 azArg[1]);
		}
		if (nArg >= 3) {
			sqlite3_snprintf(sizeof(p->rowSeparator), p->rowSeparator, "%.*s", (int)ArraySize(p->rowSeparator) - 1,
			                 azArg[2]);
		}
	} else

	    //    if( c=='v' && strncmp(azArg[0], "version", n)==0 ){
	    //        utf8_printf(p->out, "SQLite %s %s\n" /*extra-version-info*/,
	    //                    sqlite3_libversion(), sqlite3_sourceid());
	    //#if SQLITE_HAVE_ZLIB
	    //        utf8_printf(p->out, "zlib version %s\n", zlibVersion());
	    //#endif
	    //#define CTIMEOPT_VAL_(opt) #opt
	    //#define CTIMEOPT_VAL(opt) CTIMEOPT_VAL_(opt)
	    //#if defined(__clang__) && defined(__clang_major__)
	    //        utf8_printf(p->out, "clang-" CTIMEOPT_VAL(__clang_major__) "."
	    //                CTIMEOPT_VAL(__clang_minor__) "."
	    //                CTIMEOPT_VAL(__clang_patchlevel__) "\n");
	    //#elif defined(_MSC_VER)
	    //        utf8_printf(p->out, "msvc-" CTIMEOPT_VAL(_MSC_VER) "\n");
	    //#elif defined(__GNUC__) && defined(__VERSION__)
	    //    utf8_printf(p->out, "gcc-" __VERSION__ "\n");
	    //#endif
	    //    }else

	    if (c == 'w' && strncmp(azArg[0], "width", n) == 0) {
		int j;
		assert(nArg <= ArraySize(azArg));
		for (j = 1; j < nArg && j < ArraySize(p->colWidth); j++) {
			p->colWidth[j - 1] = (int)integerValue(azArg[j]);
		}
	} else

	{
		utf8_printf(stderr,
		            "Error: unknown command or invalid arguments: "
		            " \"%s\". Enter \".help\" for help\n",
		            azArg[0]);
		rc = 1;
	}

meta_command_exit:
	if (p->outCount) {
		p->outCount--;
		if (p->outCount == 0)
			output_reset(p);
	}
	return rc;
}

/*
** Return TRUE if a semicolon occurs anywhere in the first N characters
** of string z[].
*/
static int line_contains_semicolon(const char *z, int N) {
	int i;
	for (i = 0; i < N; i++) {
		if (z[i] == ';')
			return 1;
	}
	return 0;
}

/*
** Test to see if a line consists entirely of whitespace.
*/
static int _all_whitespace(const char *z) {
	for (; *z; z++) {
		if (IsSpace(z[0]))
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

/*
** Return TRUE if the line typed in is an SQL command terminator other
** than a semi-colon.  The SQL Server style "go" command is understood
** as is the Oracle "/".
*/
static int line_is_command_terminator(const char *zLine) {
	while (IsSpace(zLine[0])) {
		zLine++;
	};
	if (zLine[0] == '/' && _all_whitespace(&zLine[1])) {
		return 1; /* Oracle */
	}
	if (ToLower(zLine[0]) == 'g' && ToLower(zLine[1]) == 'o' && _all_whitespace(&zLine[2])) {
		return 1; /* SQL Server */
	}
	return 0;
}

/*
** We need a default sqlite3_complete() implementation to use in case
** the shell is compiled with SQLITE_OMIT_COMPLETE.  The default assumes
** any arbitrary text is a complete SQL statement.  This is not very
** user-friendly, but it does seem to work.
*/
#ifdef SQLITE_OMIT_COMPLETE
int sqlite3_complete(const char *zSql) {
	return 1;
}
#endif

/*
** Return true if zSql is a complete SQL statement.  Return false if it
** ends in the middle of a string literal or C-style comment.
*/
static int line_is_complete(char *zSql, int nSql) {
	int rc;
	if (zSql == 0)
		return 1;
	zSql[nSql] = ';';
	zSql[nSql + 1] = 0;
	rc = sqlite3_complete(zSql);
	zSql[nSql] = 0;
	return rc;
}

/*
** Run a single line of SQL
*/
static int runOneSqlLine(ShellState *p, char *zSql, FILE *in, int startline) {
	int rc;
	char *zErrMsg = 0;

	open_db(p, 0);
	if (ShellHasFlag(p, SHFLG_Backslash))
		resolve_backslashes(zSql);
	rc = shell_exec(p->db, zSql, shell_callback, p, &zErrMsg);
	if (rc || zErrMsg) {
		char zPrefix[100];
		if (in != 0 || !stdin_is_interactive) {
			sqlite3_snprintf(sizeof(zPrefix), zPrefix, "Error: near line %d:", startline);
		} else {
			sqlite3_snprintf(sizeof(zPrefix), zPrefix, "Error:");
		}
		if (zErrMsg != 0) {
			utf8_printf(stderr, "%s %s\n", zPrefix, zErrMsg);
			sqlite3_free(zErrMsg);
			zErrMsg = 0;
		} else {
			utf8_printf(stderr, "%s %s\n", zPrefix, sqlite3_errmsg(p->db));
		}
		return 1;
	}
	return 0;
}

/*
** Read input from *in and process it.  If *in==0 then input
** is interactive - the user is typing it it.  Otherwise, input
** is coming from a file or device.  A prompt is issued and history
** is saved only if input is interactive.  An interrupt signal will
** cause this routine to exit immediately, unless input is interactive.
**
** Return the number of errors.
*/
static int process_input(ShellState *p, FILE *in) {
	char *zLine = 0;   /* A single input line */
	char *zSql = 0;    /* Accumulated SQL text */
	int nLine;         /* Length of current line */
	int nSql = 0;      /* Bytes of zSql[] used */
	int nAlloc = 0;    /* Allocated zSql[] space */
	int nSqlPrior = 0; /* Bytes of zSql[] used by prior line */
	int rc;            /* Error code */
	int errCnt = 0;    /* Number of errors seen */
	int lineno = 0;    /* Current line number */
	int startline = 0; /* Line number for start of current input */

	while (errCnt == 0 || !bail_on_error || (in == 0 && stdin_is_interactive)) {
		fflush(p->out);
		zLine = one_input_line(in, zLine, nSql > 0);
		if (zLine == 0) {
			/* End of input */
			if (in == 0 && stdin_is_interactive)
				printf("\n");
			break;
		}
		if (seenInterrupt) {
			if (in != 0)
				break;
			seenInterrupt = 0;
		}
		lineno++;
		if (nSql == 0 && _all_whitespace(zLine)) {
			if (ShellHasFlag(p, SHFLG_Echo))
				printf("%s\n", zLine);
			continue;
		}
		if (zLine && zLine[0] == '.' && nSql == 0) {
			if (ShellHasFlag(p, SHFLG_Echo))
				printf("%s\n", zLine);
			rc = do_meta_command(zLine, p);
			if (rc == 2) { /* exit requested */
				break;
			} else if (rc) {
				errCnt++;
			}
			continue;
		}
		if (line_is_command_terminator(zLine) && line_is_complete(zSql, nSql)) {
			memcpy(zLine, ";", 2);
		}
		nLine = strlen30(zLine);
		if (nSql + nLine + 2 >= nAlloc) {
			nAlloc = nSql + nLine + 100;
			zSql = realloc(zSql, nAlloc);
			if (zSql == 0) {
				raw_printf(stderr, "Error: out of memory\n");
				exit(1);
			}
		}
		nSqlPrior = nSql;
		if (nSql == 0) {
			int i;
			for (i = 0; zLine[i] && IsSpace(zLine[i]); i++) {
			}
			assert(nAlloc > 0 && zSql != 0);
			memcpy(zSql, zLine + i, nLine + 1 - i);
			startline = lineno;
			nSql = nLine - i;
		} else {
			zSql[nSql++] = '\n';
			memcpy(zSql + nSql, zLine, nLine + 1);
			nSql += nLine;
		}
		if (nSql && line_contains_semicolon(&zSql[nSqlPrior], nSql - nSqlPrior) && sqlite3_complete(zSql)) {
			errCnt += runOneSqlLine(p, zSql, in, startline);
			nSql = 0;
			if (p->outCount) {
				output_reset(p);
				p->outCount = 0;
			} else {
				clearTempFile(p);
			}
		} else if (nSql && _all_whitespace(zSql)) {
			if (ShellHasFlag(p, SHFLG_Echo))
				printf("%s\n", zSql);
			nSql = 0;
		}
	}
	if (nSql && !_all_whitespace(zSql)) {
		runOneSqlLine(p, zSql, in, startline);
	}
	free(zSql);
	free(zLine);
	return errCnt > 0;
}

/*
** Return a pathname which is the user's home directory.  A
** 0 return indicates an error of some kind.
*/
static char *find_home_dir(int clearFlag) {
	static char *home_dir = NULL;
	if (clearFlag) {
		free(home_dir);
		home_dir = 0;
		return 0;
	}
	if (home_dir)
		return home_dir;

	{
		struct passwd *pwent;
		uid_t uid = getuid();
		if ((pwent = getpwuid(uid)) != NULL) {
			home_dir = pwent->pw_dir;
		}
	}

	if (!home_dir) {
		home_dir = getenv("HOME");
	}

	if (home_dir) {
		int n = strlen30(home_dir) + 1;
		char *z = malloc(n);
		if (z)
			memcpy(z, home_dir, n);
		home_dir = z;
	}

	return home_dir;
}

/*
** Show available command line options
*/
static const char zOptions[] = "   -ascii               set output mode to 'ascii'\n"
                               "   -column              set output mode to 'column'\n"
                               "   -cmd COMMAND         run \"COMMAND\" before reading stdin\n"
                               "   -csv                 set output mode to 'csv'\n"
                               "   -echo                print commands before execution\n"
                               "   -init FILENAME       read/process named file\n"
                               "   -[no]header          turn headers on or off\n"
                               "   -help                show this message\n"
                               "   -html                set output mode to HTML\n"
                               "   -line                set output mode to 'line'\n"
                               "   -list                set output mode to 'list'\n"
                               "   -newline SEP         set output row separator. Default: '\\n'\n"
                               "   -nullvalue TEXT      set text string for NULL values. Default ''\n"
                               "   -separator SEP       set output column separator. Default: '|'\n"
                               "   -version             show SQLite version\n";
static void usage(int showDetail) {
	//    utf8_printf(stderr,
	//                "Usage: %s [OPTIONS] FILENAME [SQL]\n"
	//                        "FILENAME is the name of an SQLite database. A new
	//                        database is created\n" "if the file does not
	//                        previously exist.\n", Argv0);
	utf8_printf(stderr, "Usage: %s [OPTIONS]\n", Argv0);

	if (showDetail) {
		utf8_printf(stderr, "OPTIONS include:\n%s", zOptions);
	} else {
		raw_printf(stderr, "Use the -help option for additional information\n");
	}
	exit(1);
}

/*
** Initialize the state information in data
*/
static void main_init(ShellState *data) {
	memset(data, 0, sizeof(*data));
	data->normalMode = data->cMode = data->mode = MODE_List;
	data->autoExplain = 1;
	memcpy(data->colSeparator, SEP_Column, 2);
	memcpy(data->rowSeparator, SEP_Row, 2);
	data->showHeader = 0;
	data->shellFlgs = SHFLG_Lookaside;
	sqlite3_config(SQLITE_CONFIG_URI, 1);
	sqlite3_config(SQLITE_CONFIG_LOG, shellLog, data);
	sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
	sqlite3_snprintf(sizeof(mainPrompt), mainPrompt, "duckdb> ");
	sqlite3_snprintf(sizeof(continuePrompt), continuePrompt, "   ...> ");
}

/*
** Output text to the console in a font that attracts extra attention.
*/
static void printBold(const char *zText) {
	printf("\033[1m%s\033[0m", zText);
}

/*
** Get the argument to an --option.  Throw an error and die if no argument
** is available.
*/
static char *cmdline_option_value(int argc, char **argv, int i) {
	if (i == argc) {
		utf8_printf(stderr, "%s: Error: missing argument to %s\n", argv[0], argv[argc - 1]);
		exit(1);
	}
	return argv[i];
}

static void process_rc(ShellState *p,                /* Configuration data */
                       const char *sqliterc_override /* Name of config file. NULL to use default */
) {
	char *home_dir = NULL;
	const char *sqliterc = sqliterc_override;
	char *zBuf = 0;
	FILE *in = NULL;

	if (sqliterc == NULL) {
		home_dir = find_home_dir(0);
		if (home_dir == 0) {
			raw_printf(stderr, "-- warning: cannot find home directory;"
			                   " cannot read ~/.duckdbrc\n");
			return;
		}
		sqlite3_initialize();
		zBuf = sqlite3_mprintf("%s/.duckdbrc", home_dir);
		sqliterc = zBuf;
	}
	in = fopen(sqliterc, "rb");
	if (in) {
		if (stdin_is_interactive) {
			utf8_printf(stderr, "-- Loading resources from %s\n", sqliterc);
		}
		process_input(p, in);
		fclose(in);
	}
	sqlite3_free(zBuf);
}

int main(int argc, char **argv) {
	char *zErrMsg = 0;
	ShellState data;
	const char *zInitFile = 0;
	int i;
	int rc = 0;
	int warnInmemoryDb = 0;
	int readStdin = 1;
	int nCmd = 0;
	char **azCmd = 0;

	setvbuf(stderr, 0, _IONBF, 0); /* Make sure stderr is unbuffered */
	stdin_is_interactive = isatty(0);
	stdout_is_console = isatty(1);

	main_init(&data);

	assert(argc >= 1 && argv && argv[0]);
	Argv0 = argv[0];

	/* Make sure we have a valid signal handler early, before anything
	 ** else is done.
	 */
	signal(SIGINT, interrupt_handler);

	/* Do an initial pass through the command-line argument to locate
	 ** the name of the database file, the name of the initialization file,
	 ** the size of the alternative malloc heap,
	 ** and the first command to execute.
	 */
	for (i = 1; i < argc; i++) {
		char *z;
		z = argv[i];
		if (z[0] != '-') {
			if (data.zDbFilename == 0) {
				data.zDbFilename = z;
			} else {
				/* Excesss arguments are interpreted as SQL (or dot-commands)
				 *and
				 ** mean that nothing is read from stdin */
				readStdin = 0;
				nCmd++;
				azCmd = realloc(azCmd, sizeof(azCmd[0]) * nCmd);
				if (azCmd == 0) {
					fprintf(stderr, "out of memory\n");
					exit(1);
				}
				azCmd[nCmd - 1] = z;
			}
		}
		if (z[1] == '-')
			z++;
		if (strcmp(z, "-separator") == 0 || strcmp(z, "-nullvalue") == 0 || strcmp(z, "-newline") == 0 ||
		    strcmp(z, "-cmd") == 0) {
			(void)cmdline_option_value(argc, argv, ++i);
		} else if (strcmp(z, "-init") == 0) {
			zInitFile = cmdline_option_value(argc, argv, ++i);
		}
	}
	if (data.zDbFilename == 0) {
#ifndef SQLITE_OMIT_MEMORYDB
		data.zDbFilename = ":memory:";
		warnInmemoryDb = argc == 1;
#else
		utf8_printf(stderr, "%s: Error: no database filename specified\n", Argv0);
		return 1;
#endif
	}
	data.out = stdout;

	/* Go ahead and open the database file if it already exists.  If the
	 ** file does not exist, delay opening it.  This prevents empty database
	 ** files from being created if a user mistypes the database name argument
	 ** to the sqlite command-line tool.
	 */
	if (access(data.zDbFilename, 0) == 0) {
		open_db(&data, 0);
	}

	process_rc(&data, zInitFile);

	/* Make a second pass through the command-line argument and set
	 ** options.  This second pass is delayed until after the initialization
	 ** file is processed so that the command-line arguments will override
	 ** settings in the initialization file.
	 */
	for (i = 1; i < argc; i++) {
		char *z = argv[i];
		if (z[0] != '-')
			continue;
		if (z[1] == '-') {
			z++;
		}
		if (strcmp(z, "-init") == 0) {
			i++;
		} else if (strcmp(z, "-html") == 0) {
			data.mode = MODE_Html;
		} else if (strcmp(z, "-list") == 0) {
			data.mode = MODE_List;
		} else if (strcmp(z, "-line") == 0) {
			data.mode = MODE_Line;
		} else if (strcmp(z, "-column") == 0) {
			data.mode = MODE_Column;
		} else if (strcmp(z, "-csv") == 0) {
			data.mode = MODE_Csv;
			memcpy(data.colSeparator, ",", 2);
		} else if (strcmp(z, "-ascii") == 0) {
			data.mode = MODE_Ascii;
			sqlite3_snprintf(sizeof(data.colSeparator), data.colSeparator, SEP_Unit);
			sqlite3_snprintf(sizeof(data.rowSeparator), data.rowSeparator, SEP_Record);
		} else if (strcmp(z, "-separator") == 0) {
			sqlite3_snprintf(sizeof(data.colSeparator), data.colSeparator, "%s", cmdline_option_value(argc, argv, ++i));
		} else if (strcmp(z, "-newline") == 0) {
			sqlite3_snprintf(sizeof(data.rowSeparator), data.rowSeparator, "%s", cmdline_option_value(argc, argv, ++i));
		} else if (strcmp(z, "-nullvalue") == 0) {
			sqlite3_snprintf(sizeof(data.nullValue), data.nullValue, "%s", cmdline_option_value(argc, argv, ++i));
		} else if (strcmp(z, "-header") == 0) {
			data.showHeader = 1;
		} else if (strcmp(z, "-noheader") == 0) {
			data.showHeader = 0;
		} else if (strcmp(z, "-echo") == 0) {
			ShellSetFlag(&data, SHFLG_Echo);
		} else if (strcmp(z, "-version") == 0) {
			printf("%s %s\n", sqlite3_libversion(), sqlite3_sourceid());
			return 0;
		} else if (strcmp(z, "-interactive") == 0) {
			stdin_is_interactive = 1;
		} else if (strcmp(z, "-help") == 0) {
			usage(1);
		} else if (strcmp(z, "-cmd") == 0) {
			/* Run commands that follow -cmd first and separately from commands
			 ** that simply appear on the command-line.  This seems goofy.  It
			 *would
			 ** be better if all commands ran in the order that they appear. But
			 ** we retain the goofy behavior for historical compatibility. */
			if (i == argc - 1)
				break;
			z = cmdline_option_value(argc, argv, ++i);
			if (z[0] == '.') {
				rc = do_meta_command(z, &data);
				if (rc && bail_on_error)
					return rc == 2 ? 0 : rc;
			} else {
				open_db(&data, 0);
				rc = shell_exec(data.db, z, shell_callback, &data, &zErrMsg);
				if (zErrMsg != 0) {
					utf8_printf(stderr, "Error: %s\n", zErrMsg);
					if (bail_on_error)
						return rc != 0 ? rc : 1;
				} else if (rc != 0) {
					utf8_printf(stderr, "Error: unable to process SQL \"%s\"\n", z);
					if (bail_on_error)
						return rc;
				}
			}
		} else {
			utf8_printf(stderr, "%s: Error: unknown option: %s\n", Argv0, z);
			raw_printf(stderr, "Use -help for a list of options.\n");
			return 1;
		}
		data.cMode = data.mode;
	}

	if (!readStdin) {
		/* Run all arguments that do not begin with '-' as if they were separate
		 ** command-line inputs, except for the argToSkip argument which
		 *contains
		 ** the database filename.
		 */
		for (i = 0; i < nCmd; i++) {
			if (azCmd[i][0] == '.') {
				rc = do_meta_command(azCmd[i], &data);
				if (rc)
					return rc == 2 ? 0 : rc;
			} else {
				open_db(&data, 0);
				rc = shell_exec(data.db, azCmd[i], shell_callback, &data, &zErrMsg);
				if (zErrMsg != 0) {
					utf8_printf(stderr, "Error: %s\n", zErrMsg);
					return rc != 0 ? rc : 1;
				} else if (rc != 0) {
					utf8_printf(stderr, "Error: unable to process SQL: %s\n", azCmd[i]);
					return rc;
				}
			}
		}
		free(azCmd);
	} else {
		/* Run commands received from standard input
		 */
		if (stdin_is_interactive) {
			char *zHome;
			char *zHistory = 0;
			int nHistory;
			printf("DuckDB %.19s\n" /*extra-version-info*/
			       "Enter \".help\" for usage hints.\n",
			       GIT_COMMIT_HASH);
			if (warnInmemoryDb) {
				printf("Connected to a ");
				printBold("transient in-memory database. \n");
			}
			zHome = find_home_dir(0);
			if (zHome) {
				nHistory = strlen30(zHome) + 20;
				if ((zHistory = malloc(nHistory)) != 0) {
					sqlite3_snprintf(nHistory, zHistory, "%s/.duckdb_history", zHome);
				}
			}
			if (zHistory) {
				shell_read_history(zHistory);
			}
#if HAVE_READLINE || HAVE_EDITLINE
			rl_attempted_completion_function = readline_completion;
#elif HAVE_LINENOISE
			linenoiseSetCompletionCallback(linenoise_completion);
#endif
			rc = process_input(&data, 0);
			if (zHistory) {
				shell_stifle_history(2000);
				shell_write_history(zHistory);
				free(zHistory);
			}
		} else {
			rc = process_input(&data, stdin);
		}
	}
	set_table_name(&data, 0);
	if (data.db) {
		sqlite3_close(data.db);
	}
	sqlite3_free(data.zFreeOnClose);
	find_home_dir(1);
	output_reset(&data);
	data.doXdgOpen = 0;
	clearTempFile(&data);
	return rc;
}
