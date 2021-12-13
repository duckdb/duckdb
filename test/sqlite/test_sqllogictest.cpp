/*
** Copyright (c) 2008 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License version 2 as published by the Free Software Foundation.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
**
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*******************************************************************************
**
** This main driver for the sqllogictest program.
*/
#include "re2/re2.h"

#include "catch.hpp"
#include "termcolor.hpp"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb/main/extension_helper.hpp"

#include "test_helpers.hpp"
#include "test_helper_extension.hpp"

#include <algorithm>
#include <functional>
#include <string.h>
#include <string>
#include <vector>
#include <iostream>
#include <thread>
#include <cfloat>

using namespace duckdb;
using namespace std;

struct Command;
struct LoopCommand;

struct LoopDefinition {
	string loop_iterator_name;
	int loop_idx;
	int loop_start;
	int loop_end;
	vector<string> tokens;
};

struct SQLLogicTestRunner {
public:
	SQLLogicTestRunner(string dbpath) : dbpath(move(dbpath)) {
		config = GetTestConfig();
		config->load_extensions = false;
	}
	~SQLLogicTestRunner();

	void ExecuteFile(string script);
	void LoadDatabase(string dbpath);

	string ReplaceKeywords(string input);

public:
	string dbpath;
	vector<string> loaded_databases;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
	unique_ptr<DBConfig> config;
	unordered_set<string> extensions;
	unordered_map<string, unique_ptr<Connection>> named_connection_map;
	bool output_hash_mode = false;
	bool output_result_mode = false;
	bool debug_mode = false;
	bool finished_processing_file = false;
	int hashThreshold = 0; /* Threshold for hashing res */
	vector<LoopCommand *> active_loops;
	unique_ptr<Command> top_level_loop;
	vector<LoopDefinition *> running_loops;
	bool original_sqlite_test = false;

	//! The map converting the labels to the hash values
	unordered_map<string, string> hash_label_map;
	unordered_map<string, unique_ptr<QueryResult>> result_label_map;

	bool InLoop() {
		return !active_loops.empty();
	}
	void ExecuteCommand(unique_ptr<Command> command);
	void StartLoop(LoopDefinition loop);
	void EndLoop();
	static string ReplaceLoopIterator(string text, string loop_iterator_name, string replacement);
	static string LoopReplacement(string text, const vector<LoopDefinition *> &loops);
};

/*
** A structure to keep track of the state of scanning the input script.
*/
typedef struct Script Script;
struct Script {
	char *zScript; /* Complete text of the input script */
	int iCur;      /* Index in zScript of start of current line */
	char *zLine;   /* Pointer to start of current line */
	int len;       /* Length of current line */
	int iNext;     /* index of start of next line */
	int nLine;     /* line number for the current line */
	int iEnd;      /* Index in zScript of '\000' at end of script */
	int startLine; /* Line number of start of current record */
	int copyFlag;  /* If true, copy lines to output as they are read */
	vector<string> tokens;

	void Clear() {
		zScript = nullptr;
		iCur = 0;
		zLine = nullptr;
		len = 0;
		iNext = 0;
		nLine = 0;
		iEnd = 0;
		startLine = 0;
		copyFlag = 0;
		tokens.clear();
	}
};

/*
** Advance the cursor to the start of the next non-comment line of the
** script.  Make p->zLine point to the start of the line.  Make p->len
** be the length of the line.  Zero-terminate the line.  Any \r at the
** end of the line is removed.
**
** Return 1 on success.  Return 0 and no-op at end-of-file.
*/
static int nextLine(Script *p) {
	int i;

	/* Loop until a non-comment line is found, or until end-of-file */
	while (1) {
		/* When we reach end-of-file, return 0 */
		if (p->iNext >= p->iEnd) {
			p->iCur = p->iEnd;
			p->zLine = &p->zScript[p->iEnd];
			p->len = 0;
			return 0;
		}

		/* Advance the cursor to the next line */
		p->iCur = p->iNext;
		p->nLine++;
		p->zLine = &p->zScript[p->iCur];
		for (i = p->iCur; i < p->iEnd && p->zScript[i] != '\n'; i++) {
		}
		p->zScript[i] = 0;
		p->len = i - p->iCur;
		p->iNext = i + 1;

		/* If the current line ends in a \r then remove the \r. */
		if (p->len > 0 && p->zScript[i - 1] == '\r') {
			p->len--;
			i--;
			p->zScript[i] = 0;
		}

		/* If the line consists of all spaces, make it an empty line */
		for (i = i - 1; i >= p->iCur && StringUtil::CharacterIsSpace(p->zScript[i]); i--) {
		}
		if (i < p->iCur) {
			p->zLine[0] = 0;
		}

		/* If the copy flag is set, write the line to standard output */
		if (p->copyFlag) {
			printf("%s\n", p->zLine);
		}

		/* If the line is not a comment line, then we are finished, so break
		** out of the loop.  If the line is a comment, the loop will repeat in
		** order to skip this line. */
		if (p->zLine[0] != '#')
			break;
	}
	return 1;
}

/*
** Look ahead to the next line and return TRUE if it is a blank line.
** But do not advance to the next line yet.
*/
static int nextIsBlank(Script *p) {
	int i = p->iNext;
	if (i >= p->iEnd)
		return 1;
	while (i < p->iEnd && StringUtil::CharacterIsSpace(p->zScript[i])) {
		if (p->zScript[i] == '\n')
			return 1;
		i++;
	}
	return 0;
}

/*
** Advance the cursor to the start of the next record.  To do this,
** first skip over the tail section of the record in which we are
** currently located, then skip over blank lines.
**
** Return 1 on success.  Return 0 at end-of-file.
*/
static int findStartOfNextRecord(Script *p) {
	/* Skip over any existing content to find a blank line */
	if (p->iCur > 0) {
		while (p->zLine[0] && p->iCur < p->iEnd) {
			nextLine(p);
		}
	} else {
		nextLine(p);
	}

	/* Skip over one or more blank lines to find the first line of the
	** new record */
	while (p->zLine[0] == 0 && p->iCur < p->iEnd) {
		nextLine(p);
	}

	/* Return 1 if we have not reached end of file. */
	return p->iCur < p->iEnd;
}

/*
** Find a single token in a string.  Return the index of the start
** of the token and the length of the token.
*/
static void findToken(const char *z, int *piStart, int *pLen) {
	int i;
	int iStart;
	for (i = 0; StringUtil::CharacterIsSpace(z[i]); i++) {
	}
	*piStart = iStart = i;
	while (z[i] && !StringUtil::CharacterIsSpace(z[i])) {
		i++;
	}
	*pLen = i - iStart;
}

#define count(X) (sizeof(X) / sizeof(X[0]))

/*
** tokenize the current line in up to 3 tokens and store those values
** into p->azToken[0], p->azToken[1], and p->azToken[2].  Record the
** current line in p->startLine.
*/
static void tokenizeLine(Script *p) {
	int i, j, k;
	int len, n;
	p->tokens.clear();

	p->startLine = p->nLine;
	for (i = j = 0; j < p->len; i++) {
		findToken(&p->zLine[j], &k, &len);
		j += k;
		n = len;
		p->tokens.push_back(string(p->zLine + j, n));
		j += n + 1;
	}
	while (p->tokens.size() < 4) {
		p->tokens.push_back(string());
	}
}

static void print_expected_result(vector<string> &values, idx_t columns, bool row_wise) {
	if (row_wise) {
		for (idx_t r = 0; r < values.size(); r++) {
			fprintf(stderr, "%s\n", values[r].c_str());
		}
	} else {
		idx_t c = 0;
		for (idx_t r = 0; r < values.size(); r++) {
			if (c != 0) {
				fprintf(stderr, "\t");
			}
			fprintf(stderr, "%s", values[r].c_str());
			c++;
			if (c >= columns) {
				fprintf(stderr, "\n");
				c = 0;
			}
		}
	}
}

static string sqllogictest_convert_value(Value value, LogicalType sql_type, bool original_sqlite_test) {
	if (value.is_null) {
		return "NULL";
	} else {
		if (original_sqlite_test) {
			// sqlite test hashes want us to convert floating point numbers to integers
			switch (sql_type.id()) {
			case LogicalTypeId::DECIMAL:
			case LogicalTypeId::FLOAT:
			case LogicalTypeId::DOUBLE:
				return value.CastAs(LogicalType::BIGINT).ToString();
			default:
				break;
			}
		}
		switch (sql_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return value.value_.boolean ? "1" : "0";
		default: {
			string str = value.ToString();
			if (str.empty()) {
				return "(empty)";
			} else {
				return str;
			}
		}
		}
	}
}

// standard result conversion: one line per value
static int duckdbConvertResult(MaterializedQueryResult &result, bool original_sqlite_test,
                               vector<string> &pazResult /* RETURN:  Array of result values */
) {
	size_t r, c;
	idx_t row_count = result.collection.Count();
	idx_t column_count = result.ColumnCount();

	pazResult.resize(row_count * column_count);
	for (r = 0; r < row_count; r++) {
		for (c = 0; c < column_count; c++) {
			auto value = result.GetValue(c, r);
			auto converted_value = sqllogictest_convert_value(value, result.types[c], original_sqlite_test);
			pazResult[r * column_count + c] = converted_value;
		}
	}
	return 0;
}

static void print_line_sep() {
	string line_sep = string(80, '=');
	std::cerr << termcolor::color<128, 128, 128> << line_sep << termcolor::reset << std::endl;
}

static void print_header(string header) {
	std::cerr << termcolor::bold << header << termcolor::reset << std::endl;
}

static void print_sql(string sql) {
	std::cerr << termcolor::bold << "SQL Query" << termcolor::reset << std::endl;
	auto tokens = Parser::Tokenize(sql);
	for (idx_t i = 0; i < tokens.size(); i++) {
		auto &token = tokens[i];
		idx_t next = i + 1 < tokens.size() ? tokens[i + 1].start : sql.size();
		// adjust the highlighting based on the type
		switch (token.type) {
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
			std::cerr << termcolor::yellow;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR:
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
			std::cerr << termcolor::green << termcolor::bold;
			break;
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
			std::cerr << termcolor::grey;
			break;
		}
		// print the current token
		std::cerr << sql.substr(token.start, next - token.start);
		// reset and move to the next token
		std::cerr << termcolor::reset;
	}
	std::cerr << std::endl;
}

static void print_error_header(const char *description, string file_name, int nline) {
	print_line_sep();
	std::cerr << termcolor::red << termcolor::bold << description << " " << termcolor::reset;
	std::cerr << termcolor::bold << "(" << file_name << ":" << nline << ")!" << termcolor::reset << std::endl;
}

static void print_result_error(vector<string> &result_values, vector<string> &values, idx_t expected_column_count,
                               bool row_wise) {
	print_header("Expected result:");
	print_line_sep();
	print_expected_result(values, expected_column_count, row_wise);
	print_line_sep();
	print_header("Actual result:");
	print_line_sep();
	print_expected_result(result_values, expected_column_count, false);
}

static void print_result_error(MaterializedQueryResult &result, vector<string> &values, idx_t expected_column_count,
                               bool row_wise) {
	print_header("Expected result:");
	print_line_sep();
	print_expected_result(values, expected_column_count, row_wise);
	print_line_sep();
	print_header("Actual result:");
	print_line_sep();
	result.Print();
}

static bool result_is_hash(string result) {
	idx_t pos = 0;
	// first parse the rows
	while (result[pos] >= '0' && result[pos] <= '9') {
		pos++;
	}
	if (pos == 0) {
		return false;
	}
	string constant_str = " values hashing to ";
	string example_hash = "acd848208cc35c7324ece9fcdd507823";
	if (pos + constant_str.size() + example_hash.size() != result.size()) {
		return false;
	}
	if (result.substr(pos, constant_str.size()) != constant_str) {
		return false;
	}
	pos += constant_str.size();
	// now parse the hash
	while ((result[pos] >= '0' && result[pos] <= '9') || (result[pos] >= 'a' && result[pos] <= 'z')) {
		pos++;
	}
	return pos == result.size();
}

static bool result_is_file(string result) {
	return StringUtil::StartsWith(result, "<FILE>:");
}

bool compare_values(MaterializedQueryResult &result, string lvalue_str, string rvalue_str, string zScriptFile,
                    int query_line, string zScript, int current_row, int current_column, vector<string> &values,
                    int expected_column_count, bool row_wise, vector<string> &result_values) {
	Value lvalue, rvalue;
	bool error = false;
	// simple first test: compare string value directly
	if (lvalue_str == rvalue_str) {
		return true;
	}
	if (StringUtil::StartsWith(rvalue_str, "<REGEX>:") || StringUtil::StartsWith(rvalue_str, "<!REGEX>:")) {
		bool want_match = StringUtil::StartsWith(rvalue_str, "<REGEX>:");
		string regex_str = StringUtil::Replace(StringUtil::Replace(rvalue_str, "<REGEX>:", ""), "<!REGEX>:", "");
		RE2::Options options;
		options.set_dot_nl(true);
		RE2 re(regex_str, options);
		if (!re.ok()) {
			print_error_header("Test error!", zScriptFile, query_line);
			print_line_sep();
			std::cerr << termcolor::red << termcolor::bold << "Failed to parse regex: " << re.error()
			          << termcolor::reset << std::endl;
			print_line_sep();
			return false;
		}
		bool regex_matches = RE2::FullMatch(lvalue_str, re);
		if (regex_matches == want_match) {
			return true;
		}
	}
	// some times require more checking (specifically floating point numbers because of inaccuracies)
	// if not equivalent we need to cast to the SQL type to verify
	auto sql_type = result.types[current_column];
	if (sql_type.IsNumeric()) {
		bool converted_lvalue = false;
		try {
			if (lvalue_str == "NULL") {
				lvalue = Value(sql_type);
			} else {
				lvalue = Value(lvalue_str).CastAs(sql_type);
			}
			converted_lvalue = true;
			if (rvalue_str == "NULL") {
				rvalue = Value(sql_type);
			} else {
				rvalue = Value(rvalue_str).CastAs(sql_type);
			}
			error = !Value::ValuesAreEqual(lvalue, rvalue);
		} catch (std::exception &ex) {
			print_error_header("Test error!", zScriptFile, query_line);
			print_line_sep();
			print_sql(zScript);
			print_line_sep();
			std::cerr << termcolor::red << termcolor::bold << "Cannot convert value "
			          << (converted_lvalue ? rvalue_str : lvalue_str) << " to type " << sql_type.ToString()
			          << termcolor::reset << std::endl;
			std::cerr << termcolor::red << termcolor::bold << ex.what() << termcolor::reset << std::endl;
			print_line_sep();
			return false;
		}
	} else if (sql_type == LogicalType::BOOLEAN) {
		auto low_r_val = StringUtil::Lower(rvalue_str);
		auto low_l_val = StringUtil::Lower(lvalue_str);

		string true_str = "true";
		string false_str = "false";
		if (low_l_val == true_str || lvalue_str == "1") {
			lvalue = Value(1);
		} else if (low_l_val == false_str || lvalue_str == "0") {
			lvalue = Value(0);
		}
		if (low_r_val == true_str || rvalue_str == "1") {
			rvalue = Value(1);
		} else if (low_r_val == false_str || rvalue_str == "0") {
			rvalue = Value(0);
		}
		error = !Value::ValuesAreEqual(lvalue, rvalue);

	} else {
		// for other types we just mark the result as incorrect
		error = true;
	}
	if (error) {
		print_error_header("Wrong result in query!", zScriptFile, query_line);
		print_line_sep();
		print_sql(zScript);
		print_line_sep();
		std::cerr << termcolor::red << termcolor::bold << "Mismatch on row " << current_row + 1 << ", column "
		          << current_column + 1 << std::endl
		          << termcolor::reset;
		std::cerr << lvalue_str << " <> " << rvalue_str << std::endl;
		print_line_sep();
		print_result_error(result_values, values, expected_column_count, row_wise);
		return false;
	}
	return true;
}

static Connection *GetConnection(DuckDB &db, unordered_map<string, unique_ptr<Connection>> &named_connection_map,
                                 string con_name) {
	auto entry = named_connection_map.find(con_name);
	if (entry == named_connection_map.end()) {
		// not found: create a new connection
		auto con = make_unique<Connection>(db);
		auto res = con.get();
		named_connection_map[con_name] = move(con);
		return res;
	}
	return entry->second.get();
}

static void query_break(int line) {
	(void)line;
}

struct Command {
	Command(SQLLogicTestRunner &runner) : runner(runner) {
	}
	virtual ~Command() {
	}

	SQLLogicTestRunner &runner;
	string connection_name;
	int query_line;
	string sql_query;
	string file_name;

	Connection *CommandConnection() {
		if (connection_name.empty()) {
			return runner.con.get();
		} else {
			return GetConnection(*runner.db, runner.named_connection_map, connection_name);
		}
	}

	unique_ptr<MaterializedQueryResult> ExecuteQuery(Connection *connection, string file_name, int query_line,
	                                                 string sql_query) {
		query_break(query_line);
		auto result = connection->Query(sql_query);

		if (!result->success) {
			TestHelperExtension::SetLastError(result->error);
		} else {
			TestHelperExtension::ClearLastError();
		}

		return result;
	}

	virtual void ExecuteInternal() = 0;
	void Execute() {
		if (runner.finished_processing_file) {
			return;
		}
		if (runner.running_loops.empty()) {
			ExecuteInternal();
			return;
		}
		auto original_query = sql_query;
		// perform the string replacement
		sql_query = SQLLogicTestRunner::LoopReplacement(sql_query, runner.running_loops);
		// execute the iterated statement
		ExecuteInternal();
		// now restore the original query
		sql_query = original_query;
	}
};

struct Statement : public Command {
	Statement(SQLLogicTestRunner &runner) : Command(runner) {
	}

	bool expect_ok;

	void ExecuteInternal() override;
};

enum class SortStyle : uint8_t { NO_SORT, ROW_SORT, VALUE_SORT };

struct Query : public Command {
	Query(SQLLogicTestRunner &runner) : Command(runner) {
	}

	idx_t expected_column_count = 0;
	SortStyle sort_style = SortStyle::NO_SORT;
	vector<string> values;
	bool query_has_label = false;
	string query_label;

	void ExecuteInternal() override;
	void ColumnCountMismatch(MaterializedQueryResult &result, int expected_column_count, bool row_wise);
	vector<string> LoadResultFromFile(string fname, vector<string> names);
};

struct RestartCommand : public Command {
	RestartCommand(SQLLogicTestRunner &runner) : Command(runner) {
	}

	void ExecuteInternal() override;
};

struct LoopCommand : public Command {
	LoopCommand(SQLLogicTestRunner &runner, LoopDefinition definition_p)
	    : Command(runner), definition(move(definition_p)) {
	}

	LoopDefinition definition;
	vector<unique_ptr<Command>> loop_commands;

	void ExecuteInternal();
};

void LoopCommand::ExecuteInternal() {
	definition.loop_idx = definition.loop_start;
	runner.running_loops.push_back(&definition);
	bool finished = false;
	while (!finished && !runner.finished_processing_file) {
		// execute the current iteration of the loop
		for (auto &statement : loop_commands) {
			statement->Execute();
		}
		definition.loop_idx++;
		if (definition.loop_idx >= definition.loop_end) {
			// finished
			break;
		}
	}
	runner.running_loops.pop_back();
}

void SQLLogicTestRunner::ExecuteCommand(unique_ptr<Command> command) {
	if (InLoop()) {
		active_loops.back()->loop_commands.push_back(move(command));
	} else {
		command->Execute();
	}
}

void SQLLogicTestRunner::StartLoop(LoopDefinition definition) {
	auto loop = make_unique<LoopCommand>(*this, move(definition));
	auto loop_ptr = loop.get();
	if (InLoop()) {
		// already in a loop: add it to the currently active loop
		active_loops.back()->loop_commands.push_back(move(loop));
	} else {
		// not in a loop yet: new top-level loop
		top_level_loop = move(loop);
	}
	active_loops.push_back(loop_ptr);
}

void SQLLogicTestRunner::EndLoop() {
	// finish a loop: pop it from the active_loop queue
	if (active_loops.empty()) {
		throw std::runtime_error("endloop without active loop!");
	}
	active_loops.pop_back();
	if (active_loops.empty()) {
		// not in a loop
		top_level_loop->Execute();
		top_level_loop.reset();
	}
}

static bool SkipErrorMessage(const string &message) {
	if (StringUtil::Contains(message, "HTTP")) {
		return true;
	}
	if (StringUtil::Contains(message, "Unable to connect")) {
		return true;
	}
	return false;
}

void Statement::ExecuteInternal() {
	auto connection = CommandConnection();

	if (runner.output_result_mode || runner.debug_mode) {
		print_line_sep();
		print_header("File " + file_name + ":" + to_string(query_line) + ")");
		print_sql(sql_query);
		print_line_sep();
	}

	query_break(query_line);
	auto result = ExecuteQuery(connection, file_name, query_line, sql_query);
	bool error = !result->success;

	if (runner.output_result_mode || runner.debug_mode) {
		result->Print();
	}

	/* Check to see if we are expecting success or failure */
	if (!expect_ok) {
		// even in the case of "statement error", we do not accept ALL errors
		// internal errors are never expected
		// neither are "unoptimized result differs from original result" errors
		bool internal_error = TestIsInternalError(result->error);
		if (!internal_error) {
			error = !error;
		} else {
			expect_ok = true;
		}
	}

	/* Report an error if the results do not match expectation */
	if (error) {
		print_error_header(!expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!", file_name,
		                   query_line);
		print_line_sep();
		print_sql(sql_query);
		print_line_sep();
		if (result) {
			result->Print();
		}
		if (expect_ok && SkipErrorMessage(result->error)) {
			runner.finished_processing_file = true;
			return;
		}
		FAIL_LINE(file_name, query_line, 0);
	}
	REQUIRE(!error);
}

void Query::ColumnCountMismatch(MaterializedQueryResult &result, int expected_column_count, bool row_wise) {
	print_error_header("Wrong column count in query!", file_name, query_line);
	std::cerr << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	          << termcolor::bold << result.ColumnCount() << termcolor::reset << " columns" << std::endl;
	print_line_sep();
	print_sql(sql_query);
	print_line_sep();
	print_result_error(result, values, expected_column_count, row_wise);
	FAIL_LINE(file_name, query_line, 0);
}

vector<string> Query::LoadResultFromFile(string fname, vector<string> names) {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("PRAGMA threads=" + to_string(std::thread::hardware_concurrency()));
	fname = StringUtil::Replace(fname, "<FILE>:", "");

	string struct_definition = "STRUCT_PACK(";
	for (idx_t i = 0; i < names.size(); i++) {
		if (i > 0) {
			struct_definition += ", ";
		}
		struct_definition += "\"" + names[i] + "\" := 'VARCHAR'";
	}
	struct_definition += ")";

	auto csv_result =
	    con.Query("SELECT * FROM read_csv('" + fname + "', header=1, sep='|', columns=" + struct_definition + ")");
	if (!csv_result->success) {
		string error = StringUtil::Format("Could not read CSV File \"%s\": %s", fname, csv_result->error);
		print_error_header(error.c_str(), file_name.c_str(), query_line);
		FAIL_LINE(file_name, query_line, 0);
	}
	expected_column_count = csv_result->ColumnCount();

	vector<string> values;
	while (true) {
		auto chunk = csv_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk->size(); r++) {
			for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
				values.push_back(chunk->GetValue(c, r).ToString());
			}
		}
	}
	return values;
}

void Query::ExecuteInternal() {
	auto connection = CommandConnection();

	if (runner.output_result_mode || runner.debug_mode) {
		print_line_sep();
		print_header("File " + file_name + ":" + to_string(query_line) + ")");
		print_sql(sql_query);
		print_line_sep();
	}

	auto result = ExecuteQuery(connection, file_name, query_line, sql_query);
	if (!result->success) {
		print_line_sep();
		fprintf(stderr, "Query unexpectedly failed (%s:%d)\n", file_name.c_str(), query_line);
		print_line_sep();
		print_sql(sql_query);
		print_line_sep();
		print_header("Actual result:");
		result->Print();
		if (SkipErrorMessage(result->error)) {
			runner.finished_processing_file = true;
			return;
		}
		FAIL_LINE(file_name, query_line, 0);
	}
	idx_t row_count = result->collection.Count();
	idx_t column_count = result->ColumnCount();
	int nResult = row_count * column_count;
	int compare_hash = query_has_label || (runner.hashThreshold > 0 && nResult > runner.hashThreshold);
	// check if the current line (the first line of the result) is a hash value
	if (values.size() == 1 && result_is_hash(values[0])) {
		compare_hash = true;
	}

	vector<string> azResult;
	duckdbConvertResult(*result, runner.original_sqlite_test, azResult);
	if (runner.output_result_mode) {
		// names
		for (idx_t c = 0; c < result->ColumnCount(); c++) {
			if (c != 0) {
				std::cerr << "\t";
			}
			std::cerr << result->names[c];
		}
		std::cerr << std::endl;
		// types
		for (idx_t c = 0; c < result->ColumnCount(); c++) {
			if (c != 0) {
				std::cerr << "\t";
			}
			std::cerr << result->types[c].ToString();
		}
		std::cerr << std::endl;
		print_line_sep();
		for (idx_t r = 0; r < result->collection.Count(); r++) {
			for (idx_t c = 0; c < result->ColumnCount(); c++) {
				if (c != 0) {
					std::cerr << "\t";
				}
				std::cerr << azResult[r * result->ColumnCount() + c];
			}
			std::cerr << std::endl;
		}
	}

	/* Do any required sorting of query results */
	if (sort_style == SortStyle::NO_SORT) {
		/* Do no sorting */
	} else if (sort_style == SortStyle::ROW_SORT) {
		/* Row-oriented sorting */
		// construct rows
		int nColumn = result->ColumnCount();
		int nRow = nResult / nColumn;
		vector<vector<string>> rows;
		rows.reserve(nRow);
		for (int r = 0; r < nRow; r++) {
			vector<string> row;
			row.reserve(nColumn);
			for (int c = 0; c < nColumn; c++) {
				row.push_back(move(azResult[r * nColumn + c]));
			}
			rows.push_back(move(row));
		}
		// sort the individual rows
		std::sort(rows.begin(), rows.end(), [](const vector<string> &a, const vector<string> &b) {
			for (size_t c = 0; c < a.size(); c++) {
				if (a[c] != b[c]) {
					return a[c] < b[c];
				}
			}
			return false;
		});
		// now reconstruct the values from the rows
		for (int r = 0; r < nRow; r++) {
			for (int c = 0; c < nColumn; c++) {
				azResult[r * nColumn + c] = move(rows[r][c]);
			}
		}
	} else if (sort_style == SortStyle::VALUE_SORT) {
		/* Sort all values independently */
		std::sort(azResult.begin(), azResult.end());
	}
	char zHash[100]; /* Storage space for hash results */
	vector<string> comparison_values;
	if (values.size() == 1 && result_is_file(values[0])) {
		comparison_values =
		    LoadResultFromFile(SQLLogicTestRunner::LoopReplacement(values[0], runner.running_loops), result->names);
	} else {
		comparison_values = values;
	}
	/* Hash the results if we are over the hash threshold or if we
	** there is a hash label */
	if (runner.output_hash_mode || compare_hash) {
		MD5Context context;
		for (int i = 0; i < nResult; i++) {
			context.Add(azResult[i]);
			context.Add("\n");
		}
		string digest = context.FinishHex();
		snprintf(zHash, sizeof(zHash), "%d values hashing to %s", nResult, digest.c_str());
		if (runner.output_hash_mode) {
			print_line_sep();
			print_sql(sql_query);
			print_line_sep();
			fprintf(stderr, "%s\n", zHash);
			print_line_sep();
			return;
		}
	}
	/* Compare subsequent lines of the script against the
	 *results
	 ** from the query.  Report an error if any differences are
	 *found.
	 */
	if (!compare_hash) {
		// check if the row/column count matches
		int original_expected_columns = expected_column_count;
		bool column_count_mismatch = false;
		if (expected_column_count != result->ColumnCount()) {
			// expected column count is different from the count found in the result
			// we try to keep going with the number of columns in the result
			expected_column_count = result->ColumnCount();
			column_count_mismatch = true;
		}
		idx_t expected_rows = comparison_values.size() / expected_column_count;
		// we first check the counts: if the values are equal to the amount of rows we expect the results to be row-wise
		bool row_wise = expected_column_count > 1 && comparison_values.size() == result->collection.Count();
		if (!row_wise) {
			// the counts do not match up for it to be row-wise
			// however, this can also be because the query returned an incorrect # of rows
			// we make a guess: if everything contains tabs, we still treat the input as row wise
			bool all_tabs = true;
			for (auto &val : comparison_values) {
				if (val.find('\t') == string::npos) {
					all_tabs = false;
					break;
				}
			}
			row_wise = all_tabs;
		}
		if (row_wise) {
			// values are displayed row-wise, format row wise with a tab
			expected_rows = comparison_values.size();
			row_wise = true;
		} else if (comparison_values.size() % expected_column_count != 0) {
			if (column_count_mismatch) {
				ColumnCountMismatch(*result, original_expected_columns, row_wise);
			}
			print_error_header("Error in test!", file_name, query_line);
			print_line_sep();
			fprintf(stderr, "Expected %d columns, but %d values were supplied\n", (int)expected_column_count,
			        (int)comparison_values.size());
			fprintf(stderr, "This is not cleanly divisible (i.e. the last row does not have enough values)\n");
			FAIL_LINE(file_name, query_line, 0);
		}
		if (expected_rows != result->collection.Count()) {
			if (column_count_mismatch) {
				ColumnCountMismatch(*result, original_expected_columns, row_wise);
			}
			print_error_header("Wrong row count in query!", file_name, query_line);
			std::cerr << "Expected " << termcolor::bold << expected_rows << termcolor::reset << " rows, but got "
			          << termcolor::bold << result->collection.Count() << termcolor::reset << " rows" << std::endl;
			print_line_sep();
			print_sql(sql_query);
			print_line_sep();
			print_result_error(*result, comparison_values, expected_column_count, row_wise);
			FAIL_LINE(file_name, query_line, 0);
		}

		if (row_wise) {
			int current_row = 0;
			for (int i = 0; i < nResult && i < (int)comparison_values.size(); i++) {
				// split based on tab character
				auto splits = StringUtil::Split(comparison_values[i], "\t");
				if (splits.size() != expected_column_count) {
					if (column_count_mismatch) {
						ColumnCountMismatch(*result, original_expected_columns, row_wise);
					}
					print_line_sep();
					print_error_header("Error in test! Column count mismatch after splitting on tab!", file_name,
					                   query_line);
					std::cerr << "Expected " << termcolor::bold << expected_column_count << termcolor::reset
					          << " columns, but got " << termcolor::bold << splits.size() << termcolor::reset
					          << " columns" << std::endl;
					std::cerr << "Does the result contain tab values? In that case, place every value on a single row."
					          << std::endl;
					print_line_sep();
					print_sql(sql_query);
					print_line_sep();
					FAIL_LINE(file_name, query_line, 0);
				}
				for (idx_t c = 0; c < splits.size(); c++) {
					bool success = compare_values(*result, azResult[current_row * expected_column_count + c], splits[c],
					                              file_name, query_line, sql_query, current_row, c, comparison_values,
					                              expected_column_count, row_wise, azResult);
					if (!success) {
						FAIL_LINE(file_name, query_line, 0);
					}
					// we do this just to increment the assertion counter
					REQUIRE(success);
				}
				current_row++;
			}
		} else {
			int current_row = 0, current_column = 0;
			for (int i = 0; i < nResult && i < (int)comparison_values.size(); i++) {
				bool success =
				    compare_values(*result, azResult[current_row * expected_column_count + current_column],
				                   comparison_values[i], file_name, query_line, sql_query, current_row, current_column,
				                   comparison_values, expected_column_count, row_wise, azResult);
				if (!success) {
					FAIL_LINE(file_name, query_line, 0);
				}
				// we do this just to increment the assertion counter
				REQUIRE(success);

				current_column++;
				if (current_column == (int)expected_column_count) {
					current_row++;
					current_column = 0;
				}
			}
		}
		if (column_count_mismatch) {
			print_line_sep();
			print_error_header("Wrong column count in query!", file_name, query_line);
			std::cerr << "Expected " << termcolor::bold << original_expected_columns << termcolor::reset
			          << " columns, but got " << termcolor::bold << expected_column_count << termcolor::reset
			          << " columns" << std::endl;
			print_line_sep();
			print_sql(sql_query);
			print_line_sep();
			std::cerr << "The expected result " << termcolor::bold << "matched" << termcolor::reset
			          << " the query result." << std::endl;
			std::cerr << termcolor::bold << "Suggested fix: modify header to \"" << termcolor::green << "query "
			          << string(result->ColumnCount(), 'I') << termcolor::reset << termcolor::bold << "\""
			          << termcolor::reset << std::endl;
			print_line_sep();
			FAIL_LINE(file_name, query_line, 0);
		}
	} else {
		bool hash_compare_error = false;
		if (query_has_label) {
			// the query has a label: check if the hash has already been computed
			auto entry = runner.hash_label_map.find(query_label);
			if (entry == runner.hash_label_map.end()) {
				// not computed yet: add it tot he map
				runner.hash_label_map[query_label] = string(zHash);
				runner.result_label_map[query_label] = move(result);
			} else {
				hash_compare_error = strcmp(entry->second.c_str(), zHash) != 0;
			}
		} else {
			if (values.size() <= 0) {
				print_error_header("Error in test: attempting to compare hash but no hash found!", file_name,
				                   query_line);
				FAIL_LINE(file_name, query_line, 0);
			}
			hash_compare_error = strcmp(values[0].c_str(), zHash) != 0;
		}
		if (hash_compare_error) {
			print_error_header("Wrong result hash!", file_name, query_line);
			print_line_sep();
			print_sql(sql_query);
			print_line_sep();
			print_header("Expected result:");
			print_line_sep();
			if (runner.result_label_map.find(query_label) != runner.result_label_map.end()) {
				runner.result_label_map[query_label]->Print();
			} else {
				std::cerr << "???" << std::endl;
			}
			print_header("Actual result:");
			print_line_sep();
			result->Print();
			FAIL_LINE(file_name, query_line, 0);
		}
		REQUIRE(!hash_compare_error);
	}
}

void RestartCommand::ExecuteInternal() {
	runner.LoadDatabase(runner.dbpath);
}

void SQLLogicTestRunner::LoadDatabase(string dbpath) {
	loaded_databases.push_back(dbpath);

	// restart the database with the specified db path
	db.reset();
	con.reset();
	named_connection_map.clear();
	// now re-open the current database

	db = make_unique<DuckDB>(dbpath, config.get());
	con = make_unique<Connection>(*db);

	// load any previously loaded extensions again
	for (auto &extension : extensions) {
		ExtensionHelper::LoadExtension(*db, extension);
	}
}

string SQLLogicTestRunner::ReplaceLoopIterator(string text, string loop_iterator_name, string replacement) {
	return StringUtil::Replace(text, "${" + loop_iterator_name + "}", replacement);
}

string SQLLogicTestRunner::LoopReplacement(string text, const vector<LoopDefinition *> &loops) {
	for (auto &active_loop : loops) {
		if (active_loop->tokens.empty()) {
			// regular loop
			text = ReplaceLoopIterator(text, active_loop->loop_iterator_name, to_string(active_loop->loop_idx));
		} else {
			// foreach loop
			text =
			    ReplaceLoopIterator(text, active_loop->loop_iterator_name, active_loop->tokens[active_loop->loop_idx]);
		}
	}
	return text;
}

string SQLLogicTestRunner::ReplaceKeywords(string input) {
	input = StringUtil::Replace(input, "__TEST_DIR__", TestDirectoryPath());
	input = StringUtil::Replace(input, "__WORKING_DIRECTORY__", FileSystem::GetWorkingDirectory());
	input = StringUtil::Replace(input, "__BUILD_DIRECTORY__", DUCKDB_BUILD_DIRECTORY);
	return input;
}

SQLLogicTestRunner::~SQLLogicTestRunner() {
	for (auto &loaded_path : loaded_databases) {
		if (loaded_path.empty()) {
			continue;
		}
		// only delete database files that were created during the tests
		if (!StringUtil::StartsWith(loaded_path, TestDirectoryPath())) {
			continue;
		}
		DeleteDatabase(loaded_path);
	}
}

void SQLLogicTestRunner::ExecuteFile(string script) {
	int haltOnError = 0; /* Stop on first error if true */
	const char *zDbEngine = "DuckDB";
	const char *zScriptFile = 0; /* Input script filename */
	unique_ptr<char[]> zScriptStorage;
	char *zScript;    /* Content of the script */
	long nScript;     /* Size of the script in bytes */
	long nGot;        /* Number of bytes read */
	int nErr = 0;     /* Number of errors */
	int nSkipped = 0; /* Number of SQL statements skipped */
	Script sScript;   /* Script parsing status */
	FILE *in;         /* For reading script */
	int bHt = 0;      /* True if -ht command-line option */
	int skip_level = 0;

	// for the original SQLite tests we convert floating point numbers to integers
	// for our own tests this is undesirable since it hides certain errors
	if (script.find("sqlite") != string::npos || script.find("sqllogictest") != string::npos) {
		original_sqlite_test = true;
	}

	if (!dbpath.empty()) {
		// delete the target database file, if it exists
		DeleteDatabase(dbpath);
	}

	// initialize the database with the default dbpath
	LoadDatabase(dbpath);

	/*
	** Read the entire script file contents into memory
	*/

	zScriptFile = script.c_str();
	in = fopen(zScriptFile, "rb");
	if (!in) {
		FAIL("Could not find test script '" + script + "'. Perhaps run `make sqlite`. ");
	}
	REQUIRE(in);
	fseek(in, 0L, SEEK_END);
	nScript = ftell(in);
	REQUIRE(nScript > 0);
	zScriptStorage = unique_ptr<char[]>(new char[nScript + 1]);
	if (!zScriptStorage) {
		FAIL();
	}
	zScript = zScriptStorage.get();
	fseek(in, 0L, SEEK_SET);
	nGot = fread(zScript, 1, nScript, in);
	fclose(in);
	REQUIRE(nGot <= nScript);
	zScript[nGot] = 0;

	/* Initialize the sScript structure so that the cursor will be pointing
	** to the start of the first line in the file after nextLine() is called
	** once. */
	sScript.Clear();
	sScript.zScript = zScript;
	sScript.zLine = zScript;
	sScript.iEnd = nScript;
	sScript.copyFlag = 0;

	/* Loop over all records in the file */
	while ((nErr == 0 || !haltOnError) && findStartOfNextRecord(&sScript)) {
		int bSkip = false; /* True if we should skip the current record. */

		/* Tokenizer the first line of the record.  This also records the
		** line number of the first record in sScript.startLine */
		tokenizeLine(&sScript);

		bSkip = false;
		while (sScript.tokens[0] == "skipif" || sScript.tokens[0] == "onlyif") {
			int bMatch;
			/* The "skipif" and "onlyif" modifiers allow skipping or using
			** statement or query record for a particular database engine.
			** In this way, SQL features implemented by a majority of the
			** engines can be tested without causing spurious errors for
			** engines that don't support it.
			**
			** Once this record is encountered, and the current selected
			** db interface matches the db engine specified in the record,
			** then we skip this rest of this record for "skipif". For
			** "onlyif" we skip the record if the record does not match.
			*/
			bMatch = sScript.tokens[1] == zDbEngine;
			if (sScript.tokens[0][0] == 's') {
				if (bMatch)
					bSkip = true;
			} else {
				if (!bMatch)
					bSkip = true;
			}
			nextLine(&sScript);
			tokenizeLine(&sScript);
		}
		if (bSkip) {
			nSkipped++;
			continue;
		}

		/* Figure out the record type and do appropriate processing */
		if (sScript.tokens[0] == "statement") {
			auto command = make_unique<Statement>(*this);

			/* Extract the SQL from second and subsequent lines of the
			** record.  Copy the SQL into contiguous memory at the beginning
			** of zScript - we are guaranteed to have enough space there. */
			command->file_name = zScriptFile;
			command->query_line = sScript.nLine;
			int k = 0;
			while (nextLine(&sScript) && sScript.zLine[0]) {
				if (k > 0)
					zScript[k++] = '\n';
				memmove(&zScript[k], sScript.zLine, sScript.len);
				k += sScript.len;
			}
			zScript[k] = 0;

			// perform any renames in zScript
			command->sql_query = ReplaceKeywords(zScript);

			// parse
			if (sScript.tokens[1] == "ok") {
				command->expect_ok = true;
			} else if (sScript.tokens[1] == "error") {
				command->expect_ok = false;
			} else {
				fprintf(stderr, "%s:%d: statement argument should be 'ok' or 'error'\n", zScriptFile,
				        sScript.startLine);
				FAIL();
			}

			command->connection_name = sScript.tokens[2];
			if (skip_level > 0) {
				continue;
			}
			ExecuteCommand(move(command));
		} else if (sScript.tokens[0] == "query") {
			auto command = make_unique<Query>(*this);

			int k = 0;
			int c;

			command->file_name = zScriptFile;
			command->query_line = sScript.nLine;

			/* Verify that the type string consists of one or more
			 *characters
			 ** from the set 'TIR':*/
			command->expected_column_count = 0;
			for (k = 0; (c = sScript.tokens[1][k]) != 0; k++) {
				command->expected_column_count++;
				if (c != 'T' && c != 'I' && c != 'R') {
					fprintf(stderr,
					        "%s:%d: unknown type character '%c' in type "
					        "string\n",
					        zScriptFile, sScript.startLine, c);
					nErr++;
					break;
				}
			}
			if (c != 0)
				continue;
			if (k <= 0) {
				fprintf(stderr, "%s:%d: missing type string\n", zScriptFile, sScript.startLine);
				FAIL();
			}

			/* Extract the SQL from second and subsequent lines of the
			 *record
			 ** until the first "----" line or until end of record.
			 */
			k = 0;
			while (!nextIsBlank(&sScript) && nextLine(&sScript) && sScript.zLine[0] &&
			       strcmp(sScript.zLine, "----") != 0) {
				if (k > 0)
					zScript[k++] = '\n';
				memmove(&zScript[k], sScript.zLine, sScript.len);
				k += sScript.len;
			}
			zScript[k] = 0;

			// perform any renames in zScript
			command->sql_query = ReplaceKeywords(zScript);

			// figure out the sort style/connection style
			command->sort_style = SortStyle::NO_SORT;
			if (sScript.tokens[2].empty() || sScript.tokens[2] == "nosort") {
				/* Do no sorting */
				command->sort_style = SortStyle::NO_SORT;
			} else if (sScript.tokens[2] == "rowsort") {
				/* Row-oriented sorting */
				command->sort_style = SortStyle::ROW_SORT;
			} else if (sScript.tokens[2] == "valuesort") {
				/* Sort all values independently */
				command->sort_style = SortStyle::VALUE_SORT;
			} else {
				command->connection_name = sScript.tokens[2];
			}

			/* In verify mode, first skip over the ---- line if we are
			 *still
			 ** pointing at it. */
			if (strcmp(sScript.zLine, "----") == 0) {
				nextLine(&sScript);
			}
			// read the expected result: keep reading until we encounter a blank line
			while (sScript.zLine[0]) {
				command->values.push_back(sScript.zLine);
				if (!nextLine(&sScript)) {
					break;
				}
			}
			command->query_has_label = sScript.tokens[3][0];
			command->query_label = sScript.tokens[3];
			if (skip_level > 0) {
				continue;
			}
			ExecuteCommand(move(command));
		} else if (sScript.tokens[0] == "hash-threshold") {
			/* Set the maximum number of result values that will be accepted
			** for a query.  If the number of result values exceeds this
			*number,
			** then an MD5 hash is computed of all values, and the resulting
			*hash
			** is the only result.
			**
			** If the threshold is 0, then hashing is never used.
			**
			** If a threshold was specified on the command line, ignore
			** any specifed in the script.
			*/
			if (!bHt) {
				hashThreshold = atoi(sScript.tokens[1].c_str());
			}
		} else if (sScript.tokens[0] == "halt") {
			/* Used for debugging.  Stop reading the test script and shut
			 *down.
			 ** A "halt" record can be inserted in the middle of a test
			 *script in
			 ** to run the script up to a particular point that is giving a
			 ** faulty result, then terminate at that point for analysis.
			 */
			fprintf(stderr, "%s:%d: halt\n", zScriptFile, sScript.startLine);
			break;
		} else if (sScript.tokens[0] == "mode") {
			if (sScript.tokens[1] == "output_hash") {
				output_hash_mode = true;
			} else if (sScript.tokens[1] == "output_result") {
				output_result_mode = true;
			} else if (sScript.tokens[1] == "debug") {
				debug_mode = true;
			} else if (sScript.tokens[1] == "skip") {
				skip_level++;
			} else if (sScript.tokens[1] == "unskip") {
				skip_level--;
			} else {
				fprintf(stderr, "%s:%d: unrecognized mode: '%s'\n", zScriptFile, sScript.startLine,
				        sScript.tokens[1].c_str());
				FAIL();
			}
		} else if (sScript.tokens[0] == "loop" || sScript.tokens[0] == "foreach") {
			if (skip_level > 0) {
				continue;
			}
			if (sScript.tokens[0] == "loop") {
				if (sScript.tokens[1].empty() || sScript.tokens[2].empty() || sScript.tokens[3].empty()) {
					fprintf(stderr,
					        "%s:%d: Test error: expected loop [iterator_name] [start] [end] (e.g. loop i 1 300)!\n",
					        zScriptFile, sScript.startLine);
					FAIL();
				}
				LoopDefinition def;
				def.loop_iterator_name = sScript.tokens[1];
				try {
					def.loop_start = std::stoi(sScript.tokens[2].c_str());
					def.loop_end = std::stoi(sScript.tokens[3].c_str());
				} catch (...) {
					fprintf(stderr,
					        "%s:%d: Test error: expected loop [iterator_name] [start] [end] (e.g. loop i 1 300)!\n",
					        zScriptFile, sScript.startLine);
					FAIL();
				}
				def.loop_idx = def.loop_start;
				StartLoop(def);
			} else {
				if (sScript.tokens[1].empty() || sScript.tokens[2].empty()) {
					fprintf(stderr,
					        "%s:%d: Test error: expected foreach [iterator_name] [m1] [m2] [etc...] (e.g. foreach type "
					        "integer smallint float)!\n",
					        zScriptFile, sScript.startLine);
					FAIL();
				}
				LoopDefinition def;
				def.loop_iterator_name = sScript.tokens[1];
				for (idx_t i = 2; i < sScript.tokens.size(); i++) {
					if (sScript.tokens[i].empty()) {
						break;
					}
					auto token_name = StringUtil::Lower(sScript.tokens[i]);
					StringUtil::Trim(token_name);
					bool collection = false;
					bool is_compression = token_name == "<compression>";
					bool is_all = token_name == "<alltypes>";
					bool is_numeric = is_all || token_name == "<numeric>";
					bool is_integral = is_numeric || token_name == "<integral>";
					bool is_signed = is_integral || token_name == "<signed>";
					bool is_unsigned = is_integral || token_name == "<unsigned>";
					if (is_signed) {
						def.tokens.push_back("tinyint");
						def.tokens.push_back("smallint");
						def.tokens.push_back("integer");
						def.tokens.push_back("bigint");
						def.tokens.push_back("hugeint");
						collection = true;
					}
					if (is_unsigned) {
						def.tokens.push_back("utinyint");
						def.tokens.push_back("usmallint");
						def.tokens.push_back("uinteger");
						def.tokens.push_back("ubigint");
						collection = true;
					}
					if (is_numeric) {
						def.tokens.push_back("float");
						def.tokens.push_back("double");
						collection = true;
					}
					if (is_all) {
						def.tokens.push_back("bool");
						def.tokens.push_back("interval");
						def.tokens.push_back("varchar");
						collection = true;
					}
					if (is_compression) {
						def.tokens.push_back("none");
						def.tokens.push_back("uncompressed");
						def.tokens.push_back("rle");
						def.tokens.push_back("bitpacking");
						collection = true;
					}
					if (!collection) {
						def.tokens.push_back(sScript.tokens[i]);
					}
				}
				def.loop_idx = 0;
				def.loop_start = 0;
				def.loop_end = def.tokens.size();
				StartLoop(def);
			}
		} else if (sScript.tokens[0] == "endloop") {
			if (skip_level > 0) {
				continue;
			}
			EndLoop();
		} else if (sScript.tokens[0] == "require") {
			// require command
			string param = StringUtil::Lower(sScript.tokens[1]);
			// os specific stuff
			if (param == "notmingw") {
#ifdef __MINGW32__
				return;
#endif
			} else if (param == "mingw") {
#ifndef __MINGW32__
				return;
#endif
			} else if (param == "notwindows") {
#ifdef _WIN32
				return;
#endif
			} else if (param == "windows") {
#ifndef _WIN32
				return;
#endif
			} else if (param == "longdouble") {
#if LDBL_MANT_DIG < 54
				return;
#endif
			} else if (param == "noforcestorage") {
				if (TestForceStorage()) {
					return;
				}
			} else if (param == "vector_size") {
				// require a specific vector size
				int required_vector_size = std::stoi(sScript.tokens[2].c_str());
				if (STANDARD_VECTOR_SIZE < required_vector_size) {
					// vector size is too low for this test: skip it
					return;
				}
			} else if (param == "test_helper") {
				db->LoadExtension<TestHelperExtension>();
			} else {
				auto result = ExtensionHelper::LoadExtension(*db, param);
				if (result == ExtensionLoadResult::LOADED_EXTENSION) {
					// add the extension to the list of loaded extensions
					extensions.insert(param);
				} else if (result == ExtensionLoadResult::EXTENSION_UNKNOWN) {
					fprintf(stderr, "%s:%d: unknown extension type: '%s'\n", zScriptFile, sScript.startLine,
					        sScript.tokens[1].c_str());
					FAIL();
				} else if (result == ExtensionLoadResult::NOT_LOADED) {
					// extension known but not build: skip this test
					return;
				}
			}
		} else if (sScript.tokens[0] == "load") {
			if (InLoop()) {
				fprintf(stderr, "%s:%d: load cannot be called in a loop\n", zScriptFile, sScript.startLine);
				FAIL();
			}
			bool readonly = sScript.tokens[2] == "readonly";
			dbpath = ReplaceKeywords(sScript.tokens[1]);
			if (!readonly) {
				// delete the target database file, if it exists
				DeleteDatabase(dbpath);
			}
			// set up the config file
			if (readonly) {
				config->use_temporary_directory = false;
				config->access_mode = AccessMode::READ_ONLY;
			} else {
				config->use_temporary_directory = true;
				config->access_mode = AccessMode::AUTOMATIC;
			}
			// now create the database file
			LoadDatabase(dbpath);
		} else if (sScript.tokens[0] == "restart") {
			if (dbpath.empty()) {
				fprintf(stderr, "%s:%d: cannot restart an in-memory database, did you forget to call \"load\"?\n",
				        zScriptFile, sScript.startLine);
				FAIL();
			}
			// restart the current database
			// first clear all connections
			auto command = make_unique<RestartCommand>(*this);
			ExecuteCommand(move(command));
		} else {
			/* An unrecognized record type is an error */
			fprintf(stderr, "%s:%d: unknown record type: '%s'\n", zScriptFile, sScript.startLine,
			        sScript.tokens[0].c_str());
			FAIL();
		}
	}
	if (InLoop()) {
		fprintf(stderr, "%s:%d: Missing endloop!\n", zScriptFile, sScript.startLine);
		FAIL();
	}
}

// code below traverses the test directory and makes individual test cases out
// of each script
static void listFiles(FileSystem &fs, const string &path, std::function<void(const string &)> cb) {
	fs.ListFiles(path, [&](string fname, bool is_dir) {
		string full_path = fs.JoinPath(path, fname);
		if (is_dir) {
			// recurse into directory
			listFiles(fs, full_path, cb);
		} else {
			cb(full_path);
		}
	});
}

static bool endsWith(const string &mainStr, const string &toMatch) {
	return (mainStr.size() >= toMatch.size() &&
	        mainStr.compare(mainStr.size() - toMatch.size(), toMatch.size(), toMatch) == 0);
}

static void testRunner() {
	// this is an ugly hack that uses the test case name to pass the script file
	// name if someone has a better idea...
	auto name = Catch::getResultCapture().getCurrentTestName();
	// fprintf(stderr, "%s\n", name.c_str());
	string initial_dbpath;
	if (TestForceStorage()) {
		auto storage_name = StringUtil::Replace(name, "/", "_");
		storage_name = StringUtil::Replace(storage_name, ".", "_");
		storage_name = StringUtil::Replace(storage_name, "\\", "_");
		initial_dbpath = TestCreatePath(storage_name + ".db");
	}
	SQLLogicTestRunner runner(move(initial_dbpath));
	runner.ExecuteFile(name);
}

static string ParseGroupFromPath(string file) {
	string extension = "";
	if (file.find(".test_slow") != std::string::npos) {
		// "slow" in the name indicates a slow test (i.e. only run as part of allunit)
		extension = "[.]";
	}
	if (file.find(".test_coverage") != std::string::npos) {
		// "slow" in the name indicates a slow test (i.e. only run as part of allunit)
		return "[coverage][.]";
	}
	// move backwards to the last slash
	int group_begin = -1, group_end = -1;
	for (idx_t i = file.size(); i > 0; i--) {
		if (file[i - 1] == '/' || file[i - 1] == '\\') {
			if (group_end == -1) {
				group_end = i - 1;
			} else {
				group_begin = i;
				return "[" + file.substr(group_begin, group_end - group_begin) + "]" + extension;
			}
		}
	}
	if (group_end == -1) {
		return "[" + file + "]" + extension;
	}
	return "[" + file.substr(0, group_end) + "]" + extension;
}

namespace duckdb {

void RegisterSqllogictests() {
	vector<string> excludes = {
	    "test/select1.test", // tested separately
	    "test/select2.test", "test/select3.test", "test/select4.test",
	    "test/index",                     // no index yet
	    "random/groupby/",                // having column binding issue with first
	    "random/select/slt_good_70.test", // join on not between
	    "random/expr/slt_good_10.test",   // these all fail because the AVG
	                                      // decimal rewrite
	    "random/expr/slt_good_102.test", "random/expr/slt_good_107.test", "random/expr/slt_good_108.test",
	    "random/expr/slt_good_109.test", "random/expr/slt_good_111.test", "random/expr/slt_good_112.test",
	    "random/expr/slt_good_113.test", "random/expr/slt_good_115.test", "random/expr/slt_good_116.test",
	    "random/expr/slt_good_117.test", "random/expr/slt_good_13.test", "random/expr/slt_good_15.test",
	    "random/expr/slt_good_16.test", "random/expr/slt_good_17.test", "random/expr/slt_good_19.test",
	    "random/expr/slt_good_21.test", "random/expr/slt_good_22.test", "random/expr/slt_good_24.test",
	    "random/expr/slt_good_28.test", "random/expr/slt_good_29.test", "random/expr/slt_good_3.test",
	    "random/expr/slt_good_30.test", "random/expr/slt_good_34.test", "random/expr/slt_good_38.test",
	    "random/expr/slt_good_4.test", "random/expr/slt_good_41.test", "random/expr/slt_good_44.test",
	    "random/expr/slt_good_45.test", "random/expr/slt_good_49.test", "random/expr/slt_good_52.test",
	    "random/expr/slt_good_53.test", "random/expr/slt_good_55.test", "random/expr/slt_good_59.test",
	    "random/expr/slt_good_6.test", "random/expr/slt_good_60.test", "random/expr/slt_good_63.test",
	    "random/expr/slt_good_64.test", "random/expr/slt_good_67.test", "random/expr/slt_good_69.test",
	    "random/expr/slt_good_7.test", "random/expr/slt_good_71.test", "random/expr/slt_good_72.test",
	    "random/expr/slt_good_8.test", "random/expr/slt_good_80.test", "random/expr/slt_good_82.test",
	    "random/expr/slt_good_85.test", "random/expr/slt_good_9.test", "random/expr/slt_good_90.test",
	    "random/expr/slt_good_91.test", "random/expr/slt_good_94.test", "random/expr/slt_good_95.test",
	    "random/expr/slt_good_96.test", "random/expr/slt_good_99.test", "random/aggregates/slt_good_2.test",
	    "random/aggregates/slt_good_5.test", "random/aggregates/slt_good_7.test", "random/aggregates/slt_good_9.test",
	    "random/aggregates/slt_good_17.test", "random/aggregates/slt_good_28.test",
	    "random/aggregates/slt_good_45.test", "random/aggregates/slt_good_50.test",
	    "random/aggregates/slt_good_52.test", "random/aggregates/slt_good_58.test",
	    "random/aggregates/slt_good_65.test", "random/aggregates/slt_good_66.test",
	    "random/aggregates/slt_good_76.test", "random/aggregates/slt_good_81.test",
	    "random/aggregates/slt_good_90.test", "random/aggregates/slt_good_96.test",
	    "random/aggregates/slt_good_102.test", "random/aggregates/slt_good_106.test",
	    "random/aggregates/slt_good_112.test", "random/aggregates/slt_good_118.test",
	    "third_party/sqllogictest/test/evidence/in1.test", // UNIQUE index on text
	    "evidence/slt_lang_replace.test",                  // feature not supported
	    "evidence/slt_lang_reindex.test",                  // "
	    "evidence/slt_lang_dropindex.test",                // "
	    "evidence/slt_lang_createtrigger.test",            // "
	    "evidence/slt_lang_droptrigger.test",              // "
	    "evidence/slt_lang_update.test",                   //  Multiple assignments to same column "x"
	    // these fail because of overflows in multiplications (sqlite does automatic upcasting)
	    "random/aggregates/slt_good_51.test", "random/aggregates/slt_good_73.test", "random/aggregates/slt_good_3.test",
	    "random/aggregates/slt_good_64.test", "random/aggregates/slt_good_122.test",
	    "random/aggregates/slt_good_110.test", "random/aggregates/slt_good_101.test",
	    "random/aggregates/slt_good_56.test", "random/aggregates/slt_good_75.test", "random/expr/slt_good_51.test",
	    "random/expr/slt_good_77.test", "random/expr/slt_good_66.test", "random/expr/slt_good_0.test",
	    "random/expr/slt_good_61.test", "random/expr/slt_good_47.test", "random/expr/slt_good_11.test",
	    "random/expr/slt_good_40.test", "random/expr/slt_good_42.test", "random/expr/slt_good_27.test",
	    "random/expr/slt_good_103.test", "random/expr/slt_good_75.test"};
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	listFiles(*fs, fs->JoinPath(fs->JoinPath("third_party", "sqllogictest"), "test"), [excludes](const string &path) {
		if (endsWith(path, ".test")) {
			for (auto excl : excludes) {
				if (path.find(excl) != string::npos) {
					return;
				}
			}
			REGISTER_TEST_CASE(testRunner, StringUtil::Replace(path, "\\", "/"), "[sqlitelogic][.]");
		}
	});
	listFiles(*fs, "test", [excludes](const string &path) {
		if (endsWith(path, ".test") || endsWith(path, ".test_slow") || endsWith(path, ".test_coverage")) {
			// parse the name / group from the test
			REGISTER_TEST_CASE(testRunner, StringUtil::Replace(path, "\\", "/"), ParseGroupFromPath(path));
		}
	});
}
} // namespace duckdb