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
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb.hpp"

namespace duckdb_shell {
using duckdb::make_uniq;
using duckdb::MaterializedQueryResult;
using duckdb::string;
using duckdb::StringUtil;
using duckdb::unique_ptr;
using duckdb::vector;
struct ColumnarResult;
struct RowResult;
class ColumnRenderer;
class RowRenderer;
using duckdb::atomic;
using duckdb::ErrorData;
using duckdb::KeywordHelper;
using duckdb::optional_idx;
using duckdb::optional_ptr;
using duckdb::SQLIdentifier;
using duckdb::SQLString;
using duckdb::unordered_map;
struct ShellState;

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
enum class OptionType { DEFAULT, ON, OFF };

enum class MetadataResult : uint8_t { SUCCESS = 0, FAIL = 1, EXIT = 2, PRINT_USAGE = 3 };

typedef MetadataResult (*metadata_command_t)(ShellState &state, const vector<string> &args);

struct CommandLineOption {
	const char *option;
	idx_t argument_count;
	const char *arguments;
	metadata_command_t pre_init_callback;
	metadata_command_t post_init_callback;
	const char *description;
};

struct MetadataCommand {
	const char *command;
	idx_t argument_count;
	metadata_command_t callback;
	const char *usage;
	const char *description;
	idx_t match_size;
	const char *extra_description;
};

/*
** State information about the database connection is contained in an
** instance of the following structure.
*/
struct ShellState {
public:
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
	unique_ptr<duckdb::MaterializedQueryResult> last_result;
	//! If the following flag is set, then command execution stops at an error
	bool bail_on_error = false;

	/*
	** Treat stdin as an interactive input if the following variable
	** is true.  Otherwise, assume stdin is connected to a file or pipe.
	*/
	bool stdin_is_interactive = true;

	/*
	** On Windows systems we have to know if standard output is a console
	** in order to translate UTF-8 into MBCS.  The following variable is
	** true if translation is required.
	*/
	bool stdout_is_console = true;
	bool stderr_is_console = true;

	//! True if an interrupt (Control-C) has been received.
	atomic<idx_t> seenInterrupt;
	//! Name of our program
	const char *program_name;

	//! Whether or not we are running in safe mode
	bool safe_mode = false;
	//! Whether or not we are highlighting errors
	OptionType highlight_errors = OptionType::DEFAULT;
	//! Whether or not we are highlighting results
	OptionType highlight_results = OptionType::DEFAULT;

	/*
	** Prompt strings. Initialized in main. Settable with
	**   .prompt main continue
	*/
	static constexpr idx_t MAX_PROMPT_SIZE = 20;
	char mainPrompt[MAX_PROMPT_SIZE];             /* First line prompt. default: "D "*/
	char continuePrompt[MAX_PROMPT_SIZE];         /* Continuation prompt. default: "   ...> " */
	char continuePromptSelected[MAX_PROMPT_SIZE]; /* Selected continuation prompt. default: "   ...> " */

#if defined(_WIN32) || defined(WIN32)
	bool win_utf8_mode = false;
#endif

public:
	static ShellState &Get();

	void Initialize();
	void Destroy();
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
	void UTF8WidthPrint(idx_t w, const string &str, bool right_align);
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
	template <typename... ARGS>
	void PrintF(PrintOutput stream, const string &str, ARGS... params) {
		Print(stream, StringUtil::Format(str, params...));
	}
	template <typename... ARGS>
	void PrintF(const string &str, ARGS... params) {
		PrintF(PrintOutput::STDOUT, str, std::forward<ARGS>(params)...);
	}
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
	bool StringToBool(const string &zArg);
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
	optional_ptr<const CommandLineOption> FindCommandLineOption(const string &option, string &error_msg) const;
	optional_ptr<const MetadataCommand> FindMetadataCommand(const string &option, string &error_msg) const;
	static vector<string> GetMetadataCompletions(const char *zLine, idx_t nLine);

	//! Execute a SQL query
	// On fail - print the error and returns FAILURE
	SuccessState ExecuteQuery(const string &query);
	//! Execute a SQL query and renders the result using the given renderer.
	//! On fail - prints the error and returns FAILURE
	SuccessState RenderQuery(RowRenderer &renderer, const string &query);
	SuccessState RenderQueryResult(RowRenderer &renderer, duckdb::QueryResult &result);
	bool HighlightErrors() const;
	bool HighlightResults() const;

	static MetadataResult SetNullValue(ShellState &state, const vector<string> &args);
	static MetadataResult SetSeparator(ShellState &state, const vector<string> &args);
	static MetadataResult EnableSafeMode(ShellState &state, const vector<string> &args);
	static MetadataResult ToggleTimer(ShellState &state, const vector<string> &args);
	SuccessState ChangeDirectory(const string &path);
	SuccessState ShowDatabases();
	void CloseOutputFile(FILE *file);
	FILE *OpenOutputFile(const char *zFile, int bTextMode);
	static void SetPrompt(char prompt[], const string &new_value);
	static string ModeToString(RenderMode mode);

private:
	ShellState();
};

} // namespace duckdb_shell
