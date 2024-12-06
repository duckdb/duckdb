#include "sqllogic_test_logger.hpp"
#include "duckdb/parser/parser.hpp"
#include "termcolor.hpp"
#include "sqllogic_test_runner.hpp"

namespace duckdb {

SQLLogicTestLogger::SQLLogicTestLogger(ExecuteContext &context, const Command &command)
    : log_lock(command.runner.log_lock), file_name(command.file_name), query_line(command.query_line),
      sql_query(context.sql_query) {
}

SQLLogicTestLogger::~SQLLogicTestLogger() {
}

void SQLLogicTestLogger::Log(const string &str) {
	std::cerr << str;
}

void SQLLogicTestLogger::PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise) {
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

void SQLLogicTestLogger::PrintLineSep() {
	string line_sep = string(80, '=');
	std::cerr << termcolor::color<128, 128, 128> << line_sep << termcolor::reset << std::endl;
}

void SQLLogicTestLogger::PrintHeader(string header) {
	std::cerr << termcolor::bold << header << termcolor::reset << std::endl;
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
			query += "\n";
		}
	} else {
		if (!StringUtil::EndsWith(sql_query, ";")) {
			query += ";";
		}
		query += "\n";
	}
	Log(query);
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
	PrintLineSep();
	std::cerr << termcolor::red << termcolor::bold << description << " " << termcolor::reset;
	if (!file_name.empty()) {
		std::cerr << termcolor::bold << "(" << file_name << ":" << query_line << ")!" << termcolor::reset;
	}
	std::cerr << std::endl;
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

void SQLLogicTestLogger::PrintResultError(MaterializedQueryResult &result, const vector<string> &values,
                                          idx_t expected_column_count, bool row_wise) {
	PrintHeader("Expected result:");
	PrintLineSep();
	PrintExpectedResult(values, expected_column_count, row_wise);
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	result.Print();
}

void SQLLogicTestLogger::UnexpectedFailure(MaterializedQueryResult &result) {
	PrintLineSep();
	std::cerr << "Query unexpectedly failed (" << file_name.c_str() << ":" << query_line << ")\n";
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintHeader("Actual result:");
	result.Print();
}
void SQLLogicTestLogger::OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string) {
	// names
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			std::cerr << "\t";
		}
		std::cerr << result.names[c];
	}
	std::cerr << std::endl;
	// types
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			std::cerr << "\t";
		}
		std::cerr << result.types[c].ToString();
	}
	std::cerr << std::endl;
	PrintLineSep();
	for (idx_t r = 0; r < result.RowCount(); r++) {
		for (idx_t c = 0; c < result.ColumnCount(); c++) {
			if (c != 0) {
				std::cerr << "\t";
			}
			std::cerr << result_values_string[r * result.ColumnCount() + c];
		}
		std::cerr << std::endl;
	}
}

void SQLLogicTestLogger::OutputHash(const string &hash_value) {
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	std::cerr << hash_value << std::endl;
	PrintLineSep();
}

void SQLLogicTestLogger::ColumnCountMismatch(MaterializedQueryResult &result,
                                             const vector<string> &result_values_string, idx_t expected_column_count,
                                             bool row_wise) {
	PrintErrorHeader("Wrong column count in query!");
	std::cerr << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	          << termcolor::bold << result.ColumnCount() << termcolor::reset << " columns" << std::endl;
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintResultError(result, result_values_string, expected_column_count, row_wise);
}

void SQLLogicTestLogger::NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count) {
	PrintErrorHeader("Error in test!");
	PrintLineSep();
	fprintf(stderr, "Expected %d columns, but %d values were supplied\n", (int)expected_column_count,
	        (int)actual_column_count);
	fprintf(stderr, "This is not cleanly divisible (i.e. the last row does not have enough values)\n");
}

void SQLLogicTestLogger::WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result,
                                       const vector<string> &comparison_values, idx_t expected_column_count,
                                       bool row_wise) {
	PrintErrorHeader("Wrong row count in query!");
	std::cerr << "Expected " << termcolor::bold << expected_rows << termcolor::reset << " rows, but got "
	          << termcolor::bold << result.RowCount() << termcolor::reset << " rows" << std::endl;
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintResultError(result, comparison_values, expected_column_count, row_wise);
}

void SQLLogicTestLogger::ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
                                                          MaterializedQueryResult &result) {
	PrintLineSep();
	PrintErrorHeader("Wrong column count in query!");
	std::cerr << "Expected " << termcolor::bold << original_expected_columns << termcolor::reset << " columns, but got "
	          << termcolor::bold << expected_column_count << termcolor::reset << " columns" << std::endl;
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	std::cerr << "The expected result " << termcolor::bold << "matched" << termcolor::reset << " the query result."
	          << std::endl;
	std::cerr << termcolor::bold << "Suggested fix: modify header to \"" << termcolor::green << "query "
	          << string(result.ColumnCount(), 'I') << termcolor::reset << termcolor::bold << "\"" << termcolor::reset
	          << std::endl;
	PrintLineSep();
}

void SQLLogicTestLogger::SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count) {
	PrintLineSep();
	PrintErrorHeader("Error in test! Column count mismatch after splitting on tab on row " + to_string(row_number) +
	                 "!");
	std::cerr << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	          << termcolor::bold << split_count << termcolor::reset << " columns" << std::endl;
	std::cerr << "Does the result contain tab values? In that case, place every value on a single row." << std::endl;
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
}

void SQLLogicTestLogger::WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result) {
	if (expected_result) {
		expected_result->Print();
	} else {
		std::cerr << "???" << std::endl;
	}
	PrintErrorHeader("Wrong result hash!");
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	PrintHeader("Expected result:");
	PrintLineSep();
	PrintHeader("Actual result:");
	PrintLineSep();
	result.Print();
}

void SQLLogicTestLogger::UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result) {
	PrintErrorHeader(!expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!");
	PrintLineSep();
	PrintSQL();
	PrintLineSep();
	result.Print();
}

void SQLLogicTestLogger::ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result) {
	PrintErrorHeader("Query failed, but error message did not match expected error message: " + expected_error);
	PrintLineSep();
	PrintSQL();
	PrintHeader("Actual result:");
	PrintLineSep();
	result.Print();
}

void SQLLogicTestLogger::LoadDatabaseFail(const string &dbpath, const string &message) {
	PrintErrorHeader(string(), 0, "Failed to load database " + dbpath);
	PrintLineSep();
	Log("Error message: " + message + "\n");
	PrintLineSep();
}

} // namespace duckdb
