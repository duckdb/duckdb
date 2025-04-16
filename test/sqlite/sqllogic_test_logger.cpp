#include "sqllogic_test_logger.hpp"
#include "duckdb/parser/parser.hpp"
#include "termcolor.hpp"
#include "result_helper.hpp"

namespace duckdb {

SQLLogicTestLogger::SQLLogicTestLogger(ExecuteContext &context, const Command &command)
    : log_lock(command.runner.log_lock), file_name(command.file_name), query_line(command.query_line),
      sql_query(context.sql_query) {
}

SQLLogicTestLogger::~SQLLogicTestLogger() {
}

void SQLLogicTestLogger::Log(const string &str, std::ostringstream &oss) {
	oss << str;
}

void SQLLogicTestLogger::PrintSummaryHeader(const std::string &file_name, std::ostringstream &oss) {
	oss << "\n" << GetSummaryCounter() << ". " << file_name << std::endl;
	PrintLineSep(oss);
}

void SQLLogicTestLogger::PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise,
                                             std::ostringstream &oss) {
	if (row_wise) {
		for (idx_t r = 0; r < values.size(); r++) {
			// fprintf(stderr, "%s\n", values[r].c_str());
			oss << "\n" << values[r].c_str();
		}
	} else {
		idx_t c = 0;
		for (idx_t r = 0; r < values.size(); r++) {
			if (c != 0) {
				// fprintf(stderr, "\t");
				oss << "\t";
			}
			// fprintf(stderr, "%s", values[r].c_str());
			oss << values[r].c_str();
			c++;
			if (c >= columns) {
				// fprintf(stderr, "\n");
				oss << std::endl;
				c = 0;
			}
		}
	}
}

void SQLLogicTestLogger::PrintLineSep(std::ostringstream &oss) {
	string line_sep = string(80, '=');
	oss << termcolor::color<128, 128, 128> << line_sep << termcolor::reset << std::endl;
}

void SQLLogicTestLogger::PrintHeader(string header, std::ostringstream &oss) {
	oss << termcolor::bold << header << termcolor::reset << std::endl;
}

void SQLLogicTestLogger::PrintFileHeader(std::ostringstream &oss) {
	PrintHeader("File " + file_name + ":" + to_string(query_line) + ")", oss);
}

void SQLLogicTestLogger::PrintSQL(std::ostringstream &oss) {
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
	Log(query, oss);
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

void SQLLogicTestLogger::PrintErrorHeader(const string &file_name, idx_t query_line, const string &description,
                                          std::ostringstream &oss) {
	PrintSummaryHeader(file_name, oss);
	oss << termcolor::red << termcolor::bold << description << " " << termcolor::reset;
	if (!file_name.empty()) {
		oss << termcolor::bold << "(" << file_name << ":" << query_line << ")!" << termcolor::reset;
	}
	oss << std::endl;
}

void SQLLogicTestLogger::PrintErrorHeader(const string &description, std::ostringstream &oss) {
	PrintErrorHeader(file_name, query_line, description, oss);
}

void SQLLogicTestLogger::PrintResultError(const vector<string> &result_values, const vector<string> &values,
                                          idx_t expected_column_count, bool row_wise, std::ostringstream &oss) {
	PrintHeader("Expected result:", oss);
	PrintLineSep(oss);
	PrintExpectedResult(values, expected_column_count, row_wise, oss);
	PrintLineSep(oss);
	PrintHeader("Actual result:", oss);
	PrintLineSep(oss);
	PrintExpectedResult(result_values, expected_column_count, false, oss);
}

void SQLLogicTestLogger::PrintResultString(MaterializedQueryResult &result, std::ostringstream &oss) {
	oss << result.ToString();
	// result.Print();
}

void SQLLogicTestLogger::PrintResultError(MaterializedQueryResult &result, const vector<string> &values,
                                          idx_t expected_column_count, bool row_wise, std::ostringstream &oss) {
	PrintHeader("Expected result:", oss);
	PrintLineSep(oss);
	PrintExpectedResult(values, expected_column_count, row_wise, oss);
	PrintLineSep(oss);
	PrintHeader("Actual result:", oss);
	PrintLineSep(oss);
	PrintResultString(result, oss);
}

void SQLLogicTestLogger::UnexpectedFailure(MaterializedQueryResult &result, std::ostringstream &oss) {
	PrintLineSep(oss);
	oss << "Query unexpectedly failed (" << file_name << ":" << to_string(query_line) << ")\n";
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintLineSep(oss);
	PrintHeader("Actual result:", oss);
	PrintResultString(result, oss);
	std::cerr << oss.str();
}
void SQLLogicTestLogger::OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string,
                                      std::ostringstream &oss) {
	// names
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			oss << "\t";
		}
		oss << result.names[c];
	}
	oss << std::endl;
	// types
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			oss << "\t";
		}
		oss << result.types[c].ToString();
	}
	oss << std::endl;
	PrintLineSep(oss);
	for (idx_t r = 0; r < result.RowCount(); r++) {
		for (idx_t c = 0; c < result.ColumnCount(); c++) {
			if (c != 0) {
				oss << "\t";
			}
			oss << result_values_string[r * result.ColumnCount() + c];
		}
		oss << std::endl;
	}
	std::cerr << oss.str();
}

void SQLLogicTestLogger::OutputHash(const string &hash_value, std::ostringstream &oss) {
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintLineSep(oss);
	oss << hash_value << std::endl;
	PrintLineSep(oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::ColumnCountMismatch(MaterializedQueryResult &result,
                                             const vector<string> &result_values_string, idx_t expected_column_count,
                                             bool row_wise, std::ostringstream &oss) {

	PrintErrorHeader("Wrong column count in query!", oss);
	oss << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	    << termcolor::bold << result.ColumnCount() << termcolor::reset << " columns" << std::endl;
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintLineSep(oss);
	PrintResultError(result, result_values_string, expected_column_count, row_wise, oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count,
                                             std::ostringstream &oss) {
	PrintErrorHeader("Error in test!", oss);
	PrintLineSep(oss);
	oss << "Expected " << to_string(expected_column_count) << " columns, but " << to_string(actual_column_count)
	    << " values were supplied\nThis is not cleanly divisible (i.e. the last row does not have enough values)";
	std::cerr << oss.str();
}

void SQLLogicTestLogger::WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result,
                                       const vector<string> &comparison_values, idx_t expected_column_count,
                                       bool row_wise, std::ostringstream &oss) {
	PrintErrorHeader("Wrong row count in query!", oss);
	oss << "Expected " << termcolor::bold << expected_rows << termcolor::reset << " rows, but got " << termcolor::bold
	    << result.RowCount() << termcolor::reset << " rows" << std::endl;
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintLineSep(oss);
	PrintResultError(result, comparison_values, expected_column_count, row_wise, oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
                                                          MaterializedQueryResult &result, std::ostringstream &oss) {
	PrintLineSep(oss);
	PrintErrorHeader("Wrong column count in query!", oss);
	oss << "Expected " << termcolor::bold << original_expected_columns << termcolor::reset << " columns, but got "
	    << termcolor::bold << expected_column_count << termcolor::reset << " columns" << std::endl;
	PrintSQL(oss);
	PrintLineSep(oss);
	oss << "The expected result " << termcolor::bold << "matched" << termcolor::reset << " the query result."
	    << std::endl;
	PrintLineSep(oss);
	oss << termcolor::bold << "Suggested fix: modify header to \"" << termcolor::green << "query "
	    << string(result.ColumnCount(), 'I') << termcolor::reset << termcolor::bold << "\"" << termcolor::reset
	    << std::endl;
	PrintLineSep(oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count,
                                       std::ostringstream &oss) {
	PrintLineSep(oss);
	PrintErrorHeader(
	    "Error in test! Column count mismatch after splitting on tab on row " + to_string(row_number) + "!", oss);
	oss << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	    << termcolor::bold << split_count << termcolor::reset << " columns" << std::endl;
	oss << "Does the result contain tab values? In that case, place every value on a single row.\n";
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintLineSep(oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result,
                                         std::ostringstream &oss) {
	if (expected_result) {
		expected_result->Print();
		expected_result->ToString();
	} else {
		oss << "???\n";
	}
	PrintErrorHeader("Wrong result hash!", oss);
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintLineSep(oss);
	PrintHeader("Expected result:", oss);
	PrintLineSep(oss);
	PrintHeader("Actual result:", oss);
	PrintLineSep(oss);
	PrintResultString(result, oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result, std::ostringstream &oss) {
	PrintErrorHeader(!expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!", oss);
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintLineSep(oss);
	PrintResultString(result, oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result,
                                               std::ostringstream &oss) {
	PrintErrorHeader("Query failed, but error message did not match expected error message: " + expected_error, oss);
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintHeader("Actual result:", oss);
	PrintLineSep(oss);
	PrintResultString(result, oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::InternalException(MaterializedQueryResult &result, std::ostringstream &oss) {
	PrintErrorHeader("Query failed with internal exception!", oss);
	PrintLineSep(oss);
	PrintSQL(oss);
	PrintHeader("Actual result:", oss);
	PrintLineSep(oss);
	PrintResultString(result, oss);
	std::cerr << oss.str();
}

void SQLLogicTestLogger::LoadDatabaseFail(const string &dbpath, const string &message, std::ostringstream &oss) {
	PrintErrorHeader(string(), 0, "Failed to load database " + dbpath, oss);
	PrintLineSep(oss);
	Log("Error message: " + message + "\n", oss);
	PrintLineSep(oss);
}

} // namespace duckdb
