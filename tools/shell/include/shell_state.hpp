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
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb.hpp"

enum class MetadataResult : uint8_t;

namespace duckdb_shell {
using duckdb::make_uniq;
using duckdb::unique_ptr;
using std::string;
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

enum class PrintOutput { STDOUT, STDERR };

enum class InputMode { STANDARD, FILE };

enum class LargeNumberRendering { NONE = 0, FOOTER = 1, ALL = 2, DEFAULT = 3 };

/*
** These are the allowed shellFlgs values
*/
enum class ShellFlags : uint32_t {
	SHFLG_Newlines = 0x00000010,     /* .dump --newline flag */
	SHFLG_CountChanges = 0x00000020, /* .changes setting */
	SHFLG_Echo = 0x00000040,         /* .echo or --echo setting */
	SHFLG_HeaderSet = 0x00000080     /* .header has been used */
};

enum class ShellOpenFlags { EXIT_ON_FAILURE, KEEP_ALIVE_ON_FAILURE };
enum class SuccessState { SUCCESS, FAILURE };

/*
** State information about the database connection is contained in an
** instance of the following structure.
*/
struct ShellState {
public:
	ShellState();

	unique_ptr<duckdb::DuckDB> db;            /* The database */
	unique_ptr<duckdb::Connection> conn;      /* The primary connection to the database */
	duckdb::DBConfig config;                  /* Config used for opening the database */
	uint8_t doXdgOpen = 0;                    /* Invoke start/open/xdg-open in output_reset() */
	int outCount = 0;                         /* Revert to stdout when reaching zero */
	int lineno = 0;                           /* Line number of last line read from in */
	FILE *in = nullptr;                       /* Read commands from this stream */
	FILE *out = nullptr;                      /* Write results here */
	int nErr = 0;                             /* Number of errors seen */
	RenderMode mode = RenderMode::LINE;       /* An output mode setting */
	RenderMode modePrior = RenderMode::LINE;  /* Saved mode */
	RenderMode cMode = RenderMode::LINE;      /* temporary output mode for the current query */
	RenderMode normalMode = RenderMode::LINE; /* Output mode before ".explain on" */
	bool showHeader = false;                  /* True to show column names in List or Column mode */
	uint32_t shellFlgs = 0;                   /* Various flags */
	uint32_t priorShFlgs = 0;                 /* Saved copy of flags */
	int64_t szMax = 0;                        /* --maxsize argument to .open */
	string zDestTable;                        /* Name of destination table when RenderMode::Insert */
	string zTempFile;                         /* Temporary file that might need deleting */
	string colSeparator;                      /* Column separator character for several modes */
	string rowSeparator;                      /* Row separator character for RenderMode::Ascii */
	string colSepPrior;                       /* Saved column separator */
	string rowSepPrior;                       /* Saved row separator */
	vector<int> colWidth;                     /* Requested width of each column in columnar modes */
	string nullValue;                         /* The text to print when a NULL comes back from the database */
	int columns = 0;                          /* Column-wise DuckBox rendering */
	string outfile;                           /* Filename for *out */
	string zDbFilename;                       /* name of the database file */
	FILE *pLog = nullptr;                     /* Write log output here */
	size_t max_rows = 0;                      /* The maximum number of rows to render in DuckBox mode */
	size_t max_width = 0; /* The maximum number of characters to render horizontally in DuckBox mode */
	//! Decimal separator (if any)
	char decimal_separator = '\0';
	//! Thousand separator (if any)
	char thousand_separator = '\0';
	//! When to use formatting of large numbers (in DuckBox mode)
	LargeNumberRendering large_number_rendering = LargeNumberRendering::DEFAULT;
	//! The command to execute when `-ui` is passed in
	string ui_command = "CALL start_ui()";
	idx_t last_changes = 0;
	idx_t total_changes = 0;
	bool readStdin = true;
	string initFile;

public:
	void PushOutputMode();
	void PopOutputMode();
	void OutputCSV(const char *z, int bSep);
	void PrintRowSeparator(idx_t nArg, const char *zSep, const vector<idx_t> &actualWidth);
	void PrintMarkdownSeparator(idx_t nArg, const char *zSep, const vector<duckdb::LogicalType> &colTypes,
	                            const vector<idx_t> &actualWidth);
	void OutputCString(const char *z);
	void OutputQuotedString(const char *z);
	void OutputQuotedEscapedString(const char *z);
	void OutputHexBlob(const void *pBlob, int nBlob);
	void PrintSchemaLine(const char *z, const char *zTail);
	void PrintSchemaLineN(char *z, int n, const char *zTail);
	void PrintOptionallyQuotedIdentifier(const char *z);
	void OutputJSONString(const char *z, int n);
	void PrintDashes(idx_t N);
	void UTF8WidthPrint(FILE *pOut, idx_t w, const string &str, bool right_align);
	bool SetOutputMode(const string &mode, const char *tbl_name);
	bool ImportData(const vector<string> &args);
	bool OpenDatabase(const vector<string> &args);
	bool SetOutputFile(const vector<string> &args, char output_mode);
	bool ReadFromFile(const string &file);
	bool DisplaySchemas(const vector<string> &args);
	MetadataResult DisplayEntries(const vector<string> &args, char type);
	void ShowConfiguration();

	idx_t RenderLength(const char *z);
	idx_t RenderLength(const string &str);
	void SetBinaryMode();
	void SetTextMode();
	static idx_t StringLength(const char *z);
	void SetTableName(const char *zName);
	void Print(PrintOutput output, const char *str);
	void Print(PrintOutput output, const string &str);
	void Print(const char *str);
	void Print(const string &str);
	void PrintPadded(const char *str, idx_t len);
	bool ColumnTypeIsInteger(const char *type);
	string strdup_handle_newline(const char *z);
	void ConvertColumnarResult(duckdb::QueryResult &res, ColumnarResult &result);
	unique_ptr<ColumnRenderer> GetColumnRenderer();
	unique_ptr<RowRenderer> GetRowRenderer();
	unique_ptr<RowRenderer> GetRowRenderer(RenderMode mode);
	void RenderColumnarResult(duckdb::QueryResult &res);
	vector<string> TableColumnList(const char *zTab);
	SuccessState ExecuteStatement(unique_ptr<duckdb::SQLStatement> statement);
	SuccessState RenderDuckBoxResult(duckdb::QueryResult &res);

	void PrintDatabaseError(const string &zErr);
	int RunInitialCommand(const char *sql, bool bail);
	void AddError();

	int RenderRow(RowRenderer &renderer, RowResult &result);

	SuccessState ExecuteSQL(const string &zSql);
	void RunSchemaDumpQuery(const string &zQuery);
	void RunTableDumpQuery(const string &zSelect);
	void OpenDB(ShellOpenFlags open_flags = ShellOpenFlags::EXIT_ON_FAILURE);

	void SetOrClearFlag(ShellFlags mFlag, const string &zArg);
	bool ShellHasFlag(ShellFlags flag) {
		return (shellFlgs & static_cast<uint32_t>(flag)) != 0;
	}

	void ShellSetFlag(ShellFlags flag) {
		shellFlgs |= static_cast<uint32_t>(flag);
	}

	void ShellClearFlag(ShellFlags flag) {
		shellFlgs &= ~static_cast<uint32_t>(flag);
	}
	void ResetOutput();
	void ClearTempFile();
	void NewTempFile(const char *zSuffix);
	int DoMetaCommand(const string &zLine);
	idx_t PrintHelp(const char *zPattern);

	int RunOneSqlLine(InputMode mode, char *zSql);
	string GetDefaultDuckDBRC();
	bool ProcessDuckDBRC(const char *file);
	bool ProcessFile(const string &file, bool is_duckdb_rc = false);
	int ProcessInput(InputMode mode);
	static bool SQLIsComplete(const char *zSql);
	static bool IsSpace(char c);
	static bool IsDigit(char c);
	static int64_t StringToInt(const string &arg);
	static void GenerateRandomBytes(int N, void *pBuf);
	static bool StringGlob(const char *zGlobPattern, const char *zString);
	static bool StringLike(const char *zPattern, const char *zStr, unsigned int esc);
	static void Sleep(idx_t ms);
	void PrintUsage();
#if defined(_WIN32) || defined(WIN32)
	static unique_ptr<uint8_t[]> Win32Utf8ToUnicode(const char *zText);
	static string Win32UnicodeToUtf8(void *zWideText);
	static string Win32MbcsToUtf8(const char *zText, bool useAnsi);
	static unique_ptr<uint8_t[]> Win32Utf8ToMbcs(const char *zText, bool useAnsi);
#endif

	//! Execute a SQL query
	// On fail - print the error and returns FAILURE
	SuccessState ExecuteQuery(const string &query);
	//! Execute a SQL query and renders the result using the given renderer.
	//! On fail - prints the error and returns FAILURE
	SuccessState RenderQuery(RowRenderer &renderer, const string &query);
	SuccessState RenderQueryResult(RowRenderer &renderer, duckdb::QueryResult &result);
};

} // namespace duckdb_shell
