//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <memory>

struct sqlite3;
struct sqlite3_stmt;

namespace duckdb_shell {
using std::unique_ptr;
using std::vector;
using std::string;

enum class RenderMode : uint32_t {
	LINE = 0,   /* One column per line.  Blank line between records */
	COLUMN,     /* One record per line in neat columns */
	LIST,       /* One record per line with a separator */
	SEMI,       /* Same as RenderMode::List but append ";" to each line */
	HTML,       /* Generate an XHTML table */
	INSERT,     /* Generate SQL "insert" statements */
	QUOTE,      /* Quote values as for SQL */
	TCL,        /* Generate ANSI-C or TCL quoted elements */
	CSV,        /* Quote strings, numbers are plain */
	EXPLAIN,    /* Like RenderMode::Column, but do not truncate data */
	ASCII,      /* Use ASCII unit and record separators (0x1F/0x1E) */
	PRETTY,     /* Pretty-print schemas */
	EQP,        /* Converts EXPLAIN QUERY PLAN output into a graph */
	JSON,       /* Output JSON */
	MARKDOWN,   /* Markdown formatting */
	TABLE,      /* MySQL-style table formatting */
	BOX,        /* Unicode box-drawing characters */
	LATEX,      /* Latex tabular formatting */
	TRASH,      /* Discard output */
	JSONLINES,  /* Output JSON Lines */
	DUCKBOX     /* Unicode box drawing - using DuckDB's own renderer */
};

/*
** State information about the database connection is contained in an
** instance of the following structure.
*/
struct ShellState {
  sqlite3 *db;           /* The database */
  uint8_t autoExplain;   /* Automatically turn on .explain mode */
  uint8_t openMode;      /* SHELL_OPEN_NORMAL, _APPENDVFS, or _ZIPFILE */
  uint8_t doXdgOpen;     /* Invoke start/open/xdg-open in output_reset() */
  uint8_t nEqpLevel;     /* Depth of the EQP output graph */
  uint8_t eTraceType;    /* SHELL_TRACE_* value for type of trace */
  unsigned mEqpLines;    /* Mask of veritical lines in the EQP output graph */
  int outCount;          /* Revert to stdout when reaching zero */
  int cnt;               /* Number of records displayed so far */
  int lineno;            /* Line number of last line read from in */
  int openFlags;         /* Additional flags to open.  (SQLITE_OPEN_NOFOLLOW) */
  FILE *in;              /* Read commands from this stream */
  FILE *out;             /* Write results here */
  FILE *traceOut;        /* Output for sqlite3_trace() */
  int nErr;              /* Number of errors seen */
  RenderMode mode;       /* An output mode setting */
  RenderMode modePrior;  /* Saved mode */
  RenderMode cMode;      /* temporary output mode for the current query */
  RenderMode normalMode; /* Output mode before ".explain on" */
  int writableSchema;    /* True if PRAGMA writable_schema=ON */
  int showHeader;        /* True to show column names in List or Column mode */
  int nCheck;            /* Number of ".check" commands run */
  unsigned nProgress;    /* Number of progress callbacks encountered */
  unsigned mxProgress;   /* Maximum progress callbacks before failing */
  unsigned flgProgress;  /* Flags for the progress callback */
  unsigned shellFlgs;    /* Various flags */
  unsigned priorShFlgs;  /* Saved copy of flags */
  int64_t szMax;         /* --maxsize argument to .open */
  char *zDestTable;      /* Name of destination table when RenderMode::Insert */
  char *zTempFile;       /* Temporary file that might need deleting */
  char zTestcase[30];    /* Name of current test case */
  char colSeparator[20]; /* Column separator character for several modes */
  char rowSeparator[20]; /* Row separator character for RenderMode::Ascii */
  char colSepPrior[20];  /* Saved column separator */
  char rowSepPrior[20];  /* Saved row separator */
  vector<int> colWidth;  /* Requested width of each column in columnar modes */
  char nullValue[20];    /* The text to print when a NULL comes back from
                         ** the database */
  int columns;           /* Column-wise DuckBox rendering */
  char outfile[FILENAME_MAX]; /* Filename for *out */
  const char *zDbFilename;    /* name of the database file */
  char *zFreeOnClose;         /* Filename to free when closing */
  const char *zVfs;           /* Name of VFS to use */
  sqlite3_stmt *pStmt;   /* Current statement if any. */
  FILE *pLog;            /* Write log output here */
  int *aiIndent;         /* Array of indents used in RenderMode::Explain */
  int nIndent;           /* Size of array aiIndent[] */
  int iIndent;           /* Index of current op in aiIndent[] */
  size_t max_rows;       /* The maximum number of rows to render in DuckBox mode */
  size_t max_width;      /* The maximum number of characters to render horizontally in DuckBox mode */

public:
	void outputModePush();
	void outputModePop();
	void output_csv(const char *z, int bSep);
	void print_row_separator(int nArg, const char *zSep, const vector<int> &actualWidth);
	void print_markdown_separator(int nArg, const char *zSep, const vector<int> &colTypes, const vector<int> &actualWidth);
	void set_table_name(const char *zName);
	int run_table_dump_query(const char *zSelect);
	void print_box_row_separator(
	  int nArg,
	  const char *zSep1,
	  const char *zSep2,
	  const char *zSep3,
	  const vector<int> &actualWidth
	);
	string strdup_handle_newline(const char *z);
	void exec_prepared_stmt_columnar(sqlite3_stmt *pStmt);
	char **tableColumnList(const char *zTab);
	void exec_prepared_stmt(sqlite3_stmt *pStmt);

	int shell_callback(int nArg, char **azArg, char **azCol, int *aiType);

	int shell_exec(
	  const char *zSql,                         /* SQL to be evaluated */
	  char **pzErrMsg                           /* Error msg written here */
	);
	int run_schema_dump_query(
	  const char *zQuery
	);
	void open_db(int openFlags);

	void setOrClearFlag(unsigned mFlag, const char *zArg);
	bool ShellHasFlag(int flag) {
		return (shellFlgs & flag) != 0;
	}

	void ShellSetFlag(int flag) {
		shellFlgs |= flag;
	}

	void ShellClearFlag(int flag) {
		shellFlgs &= ~flag;
	}
	void output_reset();
	void clearTempFile();
	void newTempFile(const char *zSuffix);
	int do_meta_command(char *zLine);

	int runOneSqlLine(char *zSql, int startline);
	void process_sqliterc(const char *sqliterc_override);
	int process_input();
};


}
