#include "sqllogic_test_logger.hpp"
#include "duckdb/parser/parser.hpp"
#include "termcolor.hpp"
#include "result_helper.hpp"
#include "sqllogic_test_runner.hpp"
#include "test_helpers.hpp"
#include "test_reporter.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"

namespace duckdb {

SQLLogicTestLogger::SQLLogicTestLogger(ExecuteContext &context, const Command &command)
    : connection(command.CommandConnection(context)), log_lock(command.runner.log_lock), file_name(command.file_name),
      query_line(command.query_line), sql_query(context.sql_query) {
}

SQLLogicTestLogger::~SQLLogicTestLogger() {
}

TestFailureRecord SQLLogicTestLogger::BaseRecord(TestFailureKind kind, const string &message) const {
	TestFailureRecord record;
	record.kind = kind;
	record.message = message;
	record.file = file_name;
	record.line = NumericCast<idx_t>(query_line);
	record.query = sql_query;
	return record;
}

void SQLLogicTestLogger::FillValues(const vector<string> &values, idx_t columns, vector<string> &out, bool &truncated,
                                    idx_t &rows) {
	rows = columns == 0 ? 0 : values.size() / columns;
	const idx_t copy_count = MinValue<idx_t>(values.size(), TEST_FAILURE_MAX_VALUES);
	truncated = copy_count < values.size();
	out.assign(values.begin(), values.begin() + NumericCast<int64_t>(copy_count));
}

void SQLLogicTestLogger::FillValues(MaterializedQueryResult &result, vector<string> &out, bool &truncated, idx_t &rows,
                                    idx_t &columns) {
	rows = result.RowCount();
	columns = result.ColumnCount();
	const idx_t total = rows * columns;
	const idx_t copy_count = MinValue<idx_t>(total, TEST_FAILURE_MAX_VALUES);
	truncated = copy_count < total;
	out.clear();
	out.reserve(copy_count);
	for (idx_t i = 0; i < copy_count; i++) {
		const idx_t r = columns == 0 ? 0 : i / columns;
		const idx_t c = columns == 0 ? 0 : i % columns;
		out.push_back(result.GetValue(c, r).ToString());
	}
}

vector<idx_t> SQLLogicTestLogger::ComputeMismatchRows(const vector<string> &expected, const vector<string> &actual,
                                                      idx_t columns) {
	vector<idx_t> mismatches;
	if (columns == 0 || expected.size() != actual.size()) {
		// different shapes: a row-wise comparison would pair up unrelated cells
		return mismatches;
	}
	const idx_t rows = expected.size() / columns;
	for (idx_t r = 0; r < rows; r++) {
		for (idx_t c = 0; c < columns; c++) {
			if (expected[r * columns + c] != actual[r * columns + c]) {
				mismatches.push_back(r);
				break;
			}
		}
	}
	return mismatches;
}

void SQLLogicTestLogger::Log(const string &annotation, const string &str) {
	if (!SuppressTextFailureOutput()) {
		std::cerr << annotation << str;
	}
	AppendFailure(str);
}

void SQLLogicTestLogger::AppendFailure(const string &log_message) {
	FailureSummary::Log(log_message);
}

void SQLLogicTestLogger::EmitTestEvent(const string &json_payload) {
	// The "[TEST_EVENT] " flare marks a line as a machine-readable event; the payload is a JSON
	// object (begin / end). Callers gate on EmitTestEventsEnabled() and pass a serialized object.
	// Build the whole line first so it reaches cerr as a single insertion — no interleaving.
	std::cerr << "[TEST_EVENT] " + json_payload + "\n";
}

void SQLLogicTestLogger::LogFailure(const string &log_message) {
	Log("", log_message);
}

void SQLLogicTestLogger::LogFailureAnnotation(const string &log_message) {
	const char *ci = std::getenv("CI");
	// check the value is "true" otherwise you'll see the prefix in local run outputs
	auto prefix = (ci && string(ci) == "true") ? "\n::error::" : "";
	Log(prefix, log_message);
}

void SQLLogicTestLogger::PrintSkip(const string &file_name, const string &reason) {
	// Opt-in via --emit-on-skip (SetEmitOnSkip): off by default so normal runs stay quiet.
	if (!EmitOnSkipEnabled()) {
		return;
	}
	// Stable, uncolored marker consumable by e.g. pytest collector. Emitted to std::cerr
	// (which survives subprocess capture) per skipped test, so skips are attributable
	// even inside a batched invocation.
	std::cerr << "[SKIP_TEST] " << file_name << " :: " << reason << "\n";
}

void SQLLogicTestLogger::ReportSkip(const string &file_name, const string &reason) {
	PrintSkip(file_name, reason);
	TestReporter::Get().Skip(reason);
}

void SQLLogicTestLogger::PrintSummaryHeader(const std::string &file_name, idx_t query_line) {
	auto failures_count = to_string(FailureSummary::GetSummaryCounter());
	if (std::getenv("NO_DUPLICATING_HEADERS") == 0) {
		LogFailure("\n" + failures_count + ". " + file_name + ":" + to_string(query_line) + "\n");
		PrintLineSep();
	}
}

void SQLLogicTestLogger::PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise) {
	// Cap the dump: a runaway result otherwise buries the failure it is supposed to explain.
	const idx_t print_count = MinValue<idx_t>(values.size(), TEST_FAILURE_MAX_VALUES);
	if (row_wise) {
		for (idx_t r = 0; r < print_count; r++) {
			LogFailure("\n" + values[r]);
		}
	} else {
		idx_t c = 0;
		for (idx_t r = 0; r < print_count; r++) {
			if (c != 0) {
				LogFailure("\t");
			}
			LogFailure(values[r]);
			c++;
			if (c >= columns) {
				LogFailure("\n");
				c = 0;
			}
		}
	}
	LogFailure("\n");
	if (print_count < values.size()) {
		LogFailure("[... " + to_string(values.size() - print_count) + " of " + to_string(values.size()) +
		           " values not shown]\n");
	}
}

void SQLLogicTestLogger::PrintLineSep() {
	string line_sep = string(80, '=');
	std::ostringstream oss;
	oss << termcolor::color<128, 128, 128> << line_sep << termcolor::reset << std::endl;
	LogFailure(oss.str());
}

void SQLLogicTestLogger::PrintHeader(string header) {
	std::ostringstream oss;
	oss << termcolor::bold << header << termcolor::reset << std::endl;
	LogFailure(oss.str());
}

void SQLLogicTestLogger::PrintFileHeader() {
	PrintHeader("File " + file_name + ":" + to_string(query_line) + ")");
}

void SQLLogicTestLogger::PrintSQL() {
	string query = sql_query;
	if (StringUtil::EndsWith(sql_query, "\n")) {
		// ends with a newline: don't add one
		if (!StringUtil::EndsWith(sql_query, ";\n")) {
			// no semicolon though
			query[query.size() - 1] = ';';
		}
	} else {
		if (!StringUtil::EndsWith(sql_query, ";")) {
			query += ";";
		}
	}
	Log("", query + "\n");
}

void SQLLogicTestLogger::PrintSQLFormatted() {
	std::cerr << termcolor::bold << "SQL Query" << termcolor::reset << std::endl;
	auto tokens = Parser::Tokenize(sql_query);
	for (idx_t i = 0; i < tokens.size(); i++) {
		auto &token = tokens[i];
		idx_t next = i + 1 < tokens.size() ? tokens[i + 1].start : sql_query.size();
		// adjust the highlighting based on the type
		switch (token.type) {
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER:
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_ERROR:
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_ERROR_EMPHASIS:
		case SimplifiedTokenType::SIMPLIFIED_TOKEN_ERROR_SUGGESTION:
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
		std::cerr << sql_query.substr(token.start, next - token.start);
		// reset and move to the next token
		std::cerr << termcolor::reset;
	}
	std::cerr << std::endl;
}

void SQLLogicTestLogger::PrintErrorHeader(const string &file_name, idx_t query_line, const string &description) {
	std::ostringstream oss;
	PrintSummaryHeader(file_name, query_line);
	oss << termcolor::red << termcolor::bold << description << " " << termcolor::reset;
	if (!file_name.empty()) {
		oss << termcolor::bold << "(" << file_name << ":" << query_line << ")!" << termcolor::reset;
	}
	LogFailureAnnotation(oss.str() + "\n");
}

void SQLLogicTestLogger::PrintErrorHeader(const string &description) {
	PrintErrorHeader(file_name, query_line, description);
}

void SQLLogicTestLogger::PrintResultError(const vector<string> &result_values, const vector<string> &values,
                                          idx_t expected_column_count, bool row_wise) {
	PrintHeader("Expected result:");
	PrintLineSep();
	PrintExpectedResult(values, expected_column_count, row_wise);
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintExpectedResult(result_values, expected_column_count, false);
}

string SQLLogicTestLogger::ResultToString(MaterializedQueryResult &result) {
	if (result.RowCount() < 100) {
		return result.ToString();
	}
	BoxRendererConfig config;
	config.max_rows = 100;
	config.max_width = -1;
	ClientBoxRendererContext render_context(*connection.context);
	return result.ToBox(render_context, config);
}

void SQLLogicTestLogger::PrintResultString(MaterializedQueryResult &result) {
	LogFailure(ResultToString(result));
}

void SQLLogicTestLogger::PrintResultError(MaterializedQueryResult &result, const vector<string> &values,
                                          idx_t expected_column_count, bool row_wise) {
	PrintHeader("Expected result:");
	PrintLineSep();
	PrintExpectedResult(values, expected_column_count, row_wise);
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
}

void SQLLogicTestLogger::UnexpectedFailure(MaterializedQueryResult &result) {
	std::ostringstream oss;
	PrintErrorHeader("Query unexpectedly failed (" + file_name + ":" + to_string(query_line) + ")\n");
	LogFailure(oss.str());
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::UNEXPECTED_FAILURE, "Query unexpectedly failed");
		record.error_message = result.HasError() ? result.GetError() : string();
		TestFailureRecorder::Record(std::move(record));
	}
}
void SQLLogicTestLogger::OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string) {
	// names
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			LogFailure("\t");
		}
		LogFailure(result.ColumnName(c).GetIdentifierName());
	}
	LogFailure("\n");
	// types
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			LogFailure("\t");
		}
		LogFailure(result.GetTypes()[c].ToString());
	}
	LogFailure("\n");
	PrintLineSep();
	const idx_t column_count = result.ColumnCount();
	const idx_t max_rows = column_count == 0 ? 0 : TEST_FAILURE_MAX_VALUES / column_count;
	const idx_t print_rows = MinValue<idx_t>(result.RowCount(), MaxValue<idx_t>(max_rows, 1));
	for (idx_t r = 0; r < print_rows; r++) {
		for (idx_t c = 0; c < column_count; c++) {
			if (c != 0) {
				LogFailure("\t");
			}
			LogFailure(result_values_string[r * column_count + c]);
		}
		LogFailure("\n");
	}
	if (print_rows < result.RowCount()) {
		LogFailure("[... " + to_string(result.RowCount() - print_rows) + " of " + to_string(result.RowCount()) +
		           " rows not shown]\n");
	}
}

void SQLLogicTestLogger::OutputHash(const string &hash_value) {
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	LogFailure(hash_value + "\n");
	PrintLineSep();
}

void SQLLogicTestLogger::ColumnCountMismatch(MaterializedQueryResult &result,
                                             const vector<string> &result_values_string, idx_t expected_column_count,
                                             bool row_wise) {
	std::ostringstream oss;
	PrintErrorHeader("Wrong column count in query!");
	oss << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	    << termcolor::bold << result.ColumnCount() << termcolor::reset << " columns" << std::endl;
	LogFailure(oss.str());
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintResultError(result, result_values_string, expected_column_count, row_wise);
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::WRONG_COLUMN_COUNT, "Wrong column count in query!");
		record.has_expected = true;
		FillValues(result_values_string, expected_column_count, record.expected, record.expected_truncated,
		           record.expected_rows);
		record.has_actual = true;
		idx_t actual_columns = 0;
		FillValues(result, record.actual, record.actual_truncated, record.actual_rows, actual_columns);
		record.columns = expected_column_count;
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count) {
	PrintLineSep();
	PrintErrorHeader("Error in test!");
	PrintLineSep();
	LogFailure("Expected " + to_string(expected_column_count) + " columns, but " + to_string(actual_column_count) +
	           " values were supplied\nThis is not cleanly divisible (i.e. the last row does not have enough values)");
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::NOT_CLEANLY_DIVISIBLE,
		                         "Expected " + to_string(expected_column_count) + " columns, but " +
		                             to_string(actual_column_count) + " values were supplied");
		record.columns = expected_column_count;
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result,
                                       const vector<string> &comparison_values, idx_t expected_column_count,
                                       bool row_wise) {
	std::ostringstream oss;
	PrintErrorHeader("Wrong row count in query!");
	oss << "Expected " << termcolor::bold << expected_rows << termcolor::reset << " rows, but got " << termcolor::bold
	    << result.RowCount() << termcolor::reset << " rows" << std::endl;
	LogFailure(oss.str());
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintResultError(result, comparison_values, expected_column_count, row_wise);
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::WRONG_ROW_COUNT, "Wrong row count in query!");
		record.has_expected = true;
		FillValues(comparison_values, expected_column_count, record.expected, record.expected_truncated,
		           record.expected_rows);
		record.expected_rows = expected_rows;
		record.has_actual = true;
		idx_t actual_columns = 0;
		FillValues(result, record.actual, record.actual_truncated, record.actual_rows, actual_columns);
		record.columns = expected_column_count;
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
                                                          MaterializedQueryResult &result) {
	std::ostringstream oss;
	PrintErrorHeader("Wrong column count in query!");
	oss << "Expected " << termcolor::bold << original_expected_columns << termcolor::reset << " columns, but got "
	    << termcolor::bold << expected_column_count << termcolor::reset << " columns" << std::endl;
	LogFailure(oss.str());
	oss.str("");
	oss.clear();
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	oss << "The expected result " << termcolor::bold << "matched" << termcolor::reset << " the query result."
	    << std::endl;
	LogFailure(oss.str());
	oss.str("");
	oss.clear();
	oss << termcolor::bold << "Suggested fix: modify header to \"" << termcolor::green << "query "
	    << string(result.ColumnCount(), 'I') << termcolor::reset << termcolor::bold << "\"" << termcolor::reset
	    << std::endl;
	LogFailure(oss.str());
	PrintLineSep();
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::WRONG_COLUMN_COUNT_CORRECT_RESULT, "Wrong column count in query!");
		record.columns = original_expected_columns;
		record.actual_rows = result.RowCount();
		record.suggested_fix = "query " + string(result.ColumnCount(), 'I');
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count) {
	std::ostringstream oss;
	PrintLineSep();
	PrintErrorHeader("Error in test! Column count mismatch after splitting on tab on row " + to_string(row_number) +
	                 "!");
	oss << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	    << termcolor::bold << split_count << termcolor::reset << " columns" << std::endl;
	LogFailure(oss.str());
	LogFailure("Does the result contain tab values? In that case, place every value on a single row.\n");
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::SPLIT_MISMATCH,
		                         "Column count mismatch after splitting on tab on row " + to_string(row_number));
		record.columns = expected_column_count;
		record.actual_rows = split_count;
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::WrongResultHash(const string &expected_result, MaterializedQueryResult &result,
                                         const string &expected_hash, const string &actual_hash) {
	PrintErrorHeader("Wrong result hash!");
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintHeader("Expected result:");
	if (!expected_result.empty()) {
		LogFailure(expected_result);
	} else {
		LogFailure("???\n");
	}
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::WRONG_RESULT_HASH, "Wrong result hash!");
		record.expected_hash = expected_hash;
		record.actual_hash = actual_hash;
		if (!expected_result.empty()) {
			record.has_expected = true;
			record.expected.push_back(expected_result);
		}
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result) {
	const string message = !expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!";
	PrintErrorHeader(message);
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintResultString(result);
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::UNEXPECTED_STATEMENT, message);
		if (result.HasError()) {
			record.error_message = result.GetError();
		} else {
			record.has_actual = true;
			FillValues(result, record.actual, record.actual_truncated, record.actual_rows, record.columns);
		}
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result) {
	PrintErrorHeader("Query failed, but error message did not match expected error message: " + expected_error);
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::EXPECTED_ERROR_MISMATCH,
		                         "Query failed, but error message did not match expected error message");
		record.has_expected = true;
		record.expected.push_back(expected_error);
		record.error_message = result.HasError() ? result.GetError() : string();
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::InternalException(MaterializedQueryResult &result) {
	PrintErrorHeader("Query failed with internal exception!");
	PrintLineSep();
	PrintSQL();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::INTERNAL_EXCEPTION, "Query failed with internal exception!");
		record.error_message = result.HasError() ? result.GetError() : string();
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::LoadDatabaseFail(const string &file_name, const string &dbpath, const string &message) {
	PrintErrorHeader(file_name, 0, "Failed to load database " + dbpath);
	PrintLineSep();
	LogFailure("Error message: " + message + "\n");
	PrintLineSep();
	if (TestFailureRecorder::Enabled()) {
		TestFailureRecord record;
		record.kind = TestFailureKind::LOAD_DATABASE_FAIL;
		record.message = "Failed to load database " + dbpath;
		record.file = file_name;
		record.error_message = message;
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::ValueMismatch(MaterializedQueryResult &result, const string &actual_value,
                                       const string &expected_value, idx_t row, idx_t column,
                                       const vector<string> &result_values, const vector<string> &values,
                                       idx_t expected_column_count, bool row_wise) {
	std::ostringstream oss;
	PrintErrorHeader("Wrong result in query!");
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	oss << termcolor::red << termcolor::bold << "Mismatch on row " << row + 1 << ", column "
	    << result.ColumnName(column) << "(index " << column + 1 << ")" << std::endl
	    << termcolor::reset;
	oss << actual_value << " <> " << expected_value << std::endl;
	LogFailure(oss.str());
	PrintLineSep();

	auto mismatch_rows = ComputeMismatchRows(values, result_values, expected_column_count);
	if (mismatch_rows.size() > 1) {
		// The dumps below may be truncated, so name the differing rows up front rather than leaving the
		// reader to diff two long lists by eye.
		const idx_t list_count = MinValue<idx_t>(mismatch_rows.size(), 20);
		string rows_str;
		for (idx_t i = 0; i < list_count; i++) {
			rows_str += (i == 0 ? "" : ", ") + to_string(mismatch_rows[i] + 1);
		}
		if (list_count < mismatch_rows.size()) {
			rows_str += ", ...";
		}
		LogFailure(to_string(mismatch_rows.size()) + " rows differ (1-based): " + rows_str + "\n");
		PrintLineSep();
	}
	PrintResultError(result_values, values, expected_column_count, row_wise);

	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::VALUE_MISMATCH, "Wrong result in query!");
		record.has_expected = true;
		FillValues(values, expected_column_count, record.expected, record.expected_truncated, record.expected_rows);
		record.has_actual = true;
		FillValues(result_values, expected_column_count, record.actual, record.actual_truncated, record.actual_rows);
		record.columns = expected_column_count;
		record.mismatch_rows = std::move(mismatch_rows);
		TestFailureRecorder::Record(std::move(record));
	}
}

void SQLLogicTestLogger::TestError(const string &description, const string &detail) {
	std::ostringstream oss;
	PrintErrorHeader(description);
	PrintLineSep();
	oss << termcolor::red << termcolor::bold << detail << termcolor::reset << std::endl;
	LogFailure(oss.str());
	PrintLineSep();
	if (TestFailureRecorder::Enabled()) {
		auto record = BaseRecord(TestFailureKind::OTHER, description);
		record.error_message = detail;
		TestFailureRecorder::Record(std::move(record));
	}
}

} // namespace duckdb
