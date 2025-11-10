#include "sqllogic_test_logger.hpp"
#include "duckdb/parser/parser.hpp"
#include "termcolor.hpp"
#include "result_helper.hpp"
#include "sqllogic_test_runner.hpp"
#include "test_helpers.hpp"

namespace duckdb {

SQLLogicTestLogger::SQLLogicTestLogger(ExecuteContext &context, const Command &command)
    : log_lock(command.runner.log_lock), file_name(command.file_name), query_line(command.query_line),
      sql_query(context.sql_query) {
}

SQLLogicTestLogger::~SQLLogicTestLogger() {
}

void SQLLogicTestLogger::Log(const string &annotation, const string &str) {
	std::cerr << annotation << str;
	AppendFailure(str);
}

void SQLLogicTestLogger::AppendFailure(const string &log_message) {
	FailureSummary::Log(log_message);
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

void SQLLogicTestLogger::PrintSummaryHeader(const std::string &file_name, idx_t query_line) {
	auto failures_count = to_string(FailureSummary::GetSummaryCounter());
	if (std::getenv("NO_DUPLICATING_HEADERS") == 0) {
		LogFailure("\n" + failures_count + ". " + file_name + ":" + to_string(query_line) + "\n");
		PrintLineSep();
	}
}

void SQLLogicTestLogger::PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise) {
	if (row_wise) {
		for (idx_t r = 0; r < values.size(); r++) {
			LogFailure("\n" + values[r]);
		}
	} else {
		idx_t c = 0;
		for (idx_t r = 0; r < values.size(); r++) {
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

void SQLLogicTestLogger::PrintResultString(MaterializedQueryResult &result) {
	LogFailure(result.ToString());
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
}
void SQLLogicTestLogger::OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string) {
	// names
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			LogFailure("\t");
		}
		LogFailure(result.names[c]);
	}
	LogFailure("\n");
	// types
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			LogFailure("\t");
		}
		LogFailure(result.types[c].ToString());
	}
	LogFailure("\n");
	PrintLineSep();
	for (idx_t r = 0; r < result.RowCount(); r++) {
		for (idx_t c = 0; c < result.ColumnCount(); c++) {
			if (c != 0) {
				LogFailure("\t");
			}
			LogFailure(result_values_string[r * result.ColumnCount() + c]);
		}
		LogFailure("\n");
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
}

void SQLLogicTestLogger::NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count) {
	PrintLineSep();
	PrintErrorHeader("Error in test!");
	PrintLineSep();
	LogFailure("Expected " + to_string(expected_column_count) + " columns, but " + to_string(actual_column_count) +
	           " values were supplied\nThis is not cleanly divisible (i.e. the last row does not have enough values)");
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
}

void SQLLogicTestLogger::WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result,
                                         const string &expected_hash, const string &actual_hash) {
	if (expected_result) {
		expected_result->Print();
		expected_result->ToString();
	} else {
		LogFailure("???\n");
	}
	PrintErrorHeader("Wrong result hash!");
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintHeader("Expected result:");
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
}

void SQLLogicTestLogger::UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result) {
	PrintErrorHeader(!expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!");
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintResultString(result);
}

void SQLLogicTestLogger::ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result) {
	PrintErrorHeader("Query failed, but error message did not match expected error message: " + expected_error);
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
}

void SQLLogicTestLogger::InternalException(MaterializedQueryResult &result) {
	PrintErrorHeader("Query failed with internal exception!");
	PrintLineSep();
	PrintSQL();
	PrintHeader("Actual result:");
	PrintLineSep();
	PrintResultString(result);
}

void SQLLogicTestLogger::LoadDatabaseFail(const string &file_name, const string &dbpath, const string &message) {
	PrintErrorHeader(file_name, 0, "Failed to load database " + dbpath);
	PrintLineSep();
	LogFailure("Error message: " + message + "\n");
	PrintLineSep();
}

} // namespace duckdb
