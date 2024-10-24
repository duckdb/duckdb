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
enum class MetadataResult : uint8_t;

namespace duckdb_shell {
using std::string;
using std::unique_ptr;
using std::vector;
struct ColumnarResult;
struct RowResult;
class ColumnRenderer;
class RowRenderer;

using idx_t = uint64_t;

enum class RenderMode : uint32_t {
	LINE = 0,  /* One column per line.  Blank line between records */
	COLUMN,    /* One record per line in neat columns */
	LIST,      /* One record per line with a separator */
	SEMI,      /* Same as RenderMode::List but append ";" to each line */
	HTML,      /* Generate an XHTML table */
	INSERT,    /* Generate SQL "insert" statements */
	QUOTE,     /* Quote values as for SQL */
	TCL,       /* Generate ANSI-C or TCL quoted elements */
	CSV,       /* Quote strings, numbers are plain */
	EXPLAIN,   /* Like RenderMode::Column, but do not truncate data */
	ASCII,     /* Use ASCII unit and record separators (0x1F/0x1E) */
	PRETTY,    /* Pretty-print schemas */
	EQP,       /* Converts EXPLAIN QUERY PLAN output into a graph */
	JSON,      /* Output JSON */
	MARKDOWN,  /* Markdown formatting */
	TABLE,     /* MySQL-style table formatting */
	BOX,       /* Unicode box-drawing characters */
	LATEX,     /* Latex tabular formatting */
	TRASH,     /* Discard output */
	JSONLINES, /* Output JSON Lines */
	DUCKBOX    /* Unicode box drawing - using DuckDB's own renderer */
};

/*
** These are the allowed shellFlgs values
*/
#define SHFLG_Pagecache     0x00000001 /* The --pagecache option is used */
#define SHFLG_Lookaside     0x00000002 /* Lookaside memory is used */
#define SHFLG_Backslash     0x00000004 /* The --backslash option is used */
#define SHFLG_PreserveRowid 0x00000008 /* .dump preserves rowid values */
#define SHFLG_Newlines      0x00000010 /* .dump --newline flag */
#define SHFLG_CountChanges  0x00000020 /* .changes setting */
#define SHFLG_Echo          0x00000040 /* .echo or --echo setting */
#define SHFLG_HeaderSet     0x00000080 /* .header has been used */

/* ctype macros that work with signed characters */
#define IsSpace(X) isspace((unsigned char)X)
#define IsDigit(X) isdigit((unsigned char)X)
#define ToLower(X) (char)tolower((unsigned char)X)

/*
** State information about the database connection is contained in an
** instance of the following structure.
*/
struct ShellState {
	sqlite3 *db = nullptr;                    /* The database */
	uint8_t autoExplain = 0;                  /* Automatically turn on .explain mode */
	uint8_t openMode = 0;                     /* SHELL_OPEN_NORMAL, _APPENDVFS, or _ZIPFILE */
	uint8_t doXdgOpen = 0;                    /* Invoke start/open/xdg-open in output_reset() */
	int outCount = 0;                         /* Revert to stdout when reaching zero */
	int lineno = 0;                           /* Line number of last line read from in */
	int openFlags = 0;                        /* Additional flags to open.  (SQLITE_OPEN_NOFOLLOW) */
	FILE *in = nullptr;                       /* Read commands from this stream */
	FILE *out = nullptr;                      /* Write results here */
	int nErr = 0;                             /* Number of errors seen */
	RenderMode mode = RenderMode::LINE;       /* An output mode setting */
	RenderMode modePrior = RenderMode::LINE;  /* Saved mode */
	RenderMode cMode = RenderMode::LINE;      /* temporary output mode for the current query */
	RenderMode normalMode = RenderMode::LINE; /* Output mode before ".explain on" */
	int showHeader = 0;                       /* True to show column names in List or Column mode */
	unsigned shellFlgs = 0;                   /* Various flags */
	unsigned priorShFlgs = 0;                 /* Saved copy of flags */
	int64_t szMax = 0;                        /* --maxsize argument to .open */
	char *zDestTable = nullptr;               /* Name of destination table when RenderMode::Insert */
	char *zTempFile = nullptr;                /* Temporary file that might need deleting */
	string colSeparator;                      /* Column separator character for several modes */
	string rowSeparator;                      /* Row separator character for RenderMode::Ascii */
	string colSepPrior;                       /* Saved column separator */
	string rowSepPrior;                       /* Saved row separator */
	vector<int> colWidth;                     /* Requested width of each column in columnar modes */
	string nullValue;                         /* The text to print when a NULL comes back from the database */
	int columns = 0;                          /* Column-wise DuckBox rendering */
	string outfile;                           /* Filename for *out */
	string zDbFilename;                       /* name of the database file */
	sqlite3_stmt *pStmt = nullptr;            /* Current statement if any. */
	FILE *pLog = nullptr;                     /* Write log output here */
	size_t max_rows = 0;                      /* The maximum number of rows to render in DuckBox mode */
	size_t max_width = 0; /* The maximum number of characters to render horizontally in DuckBox mode */

public:
	void outputModePush();
	void outputModePop();
	void output_csv(const char *z, int bSep);
	void print_row_separator(int nArg, const char *zSep, const vector<int> &actualWidth);
	void print_markdown_separator(int nArg, const char *zSep, const vector<int> &colTypes,
	                              const vector<int> &actualWidth);
	void output_c_string(const char *z);
	void output_quoted_string(const char *z);
	void output_quoted_escaped_string(const char *z);
	void output_hex_blob(const void *pBlob, int nBlob);
	void printSchemaLine(const char *z, const char *zTail);
	void printSchemaLineN(char *z, int n, const char *zTail);
	void PrintOptionallyQuotedIdentifier(const char *z);
	int isNumber(const char *z, int *realnum);
	void output_json_string(const char *z, int n);
	void print_dashes(int N);
	void utf8_width_print(FILE *pOut, int w, const string &str);
	bool SetOutputMode(const char *mode, const char *tbl_name);
	bool ImportData(const char **azArg, idx_t nArg);
	bool OpenDatabase(const char **azArg, idx_t nArg);
	bool SetOutputFile(const char **azArg, idx_t nArg, char output_mode);
	bool ReadFromFile(const string &file);
	bool DisplaySchemas(const char **azArg, idx_t nArg);
	MetadataResult DisplayEntries(const char **azArg, idx_t nArg, char type);
	void ShowConfiguration();

	int strlenChar(const char *z);
	int strlenChar(const string &str);
	void SetBinaryMode();
	void SetTextMode();
	static int StringLength(const char *z);
	void set_table_name(const char *zName);
	int run_table_dump_query(const char *zSelect);
	void PrintValue(const char *str);
	void Print(const char *str);
	void Print(const string &str);
	void PrintPadded(const char *str, idx_t len);
	bool column_type_is_integer(const char *type);
	string strdup_handle_newline(const char *z);
	ColumnarResult ExecuteColumnar(sqlite3_stmt *pStmt);
	unique_ptr<ColumnRenderer> GetColumnRenderer();
	unique_ptr<RowRenderer> GetRowRenderer();
	unique_ptr<RowRenderer> GetRowRenderer(RenderMode mode);
	void exec_prepared_stmt_columnar(sqlite3_stmt *pStmt);
	char **tableColumnList(const char *zTab);
	void exec_prepared_stmt(sqlite3_stmt *pStmt);

	int shell_callback(RowRenderer &renderer, RowResult &result);

	int shell_exec(const char *zSql, /* SQL to be evaluated */
	               char **pzErrMsg   /* Error msg written here */
	);
	int run_schema_dump_query(const char *zQuery);
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

} // namespace duckdb_shell
