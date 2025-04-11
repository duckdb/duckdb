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

std::stringstream SQLLogicTestLogger::Log(const string &str) {
	std::stringstream log_message;
	log_message << str;
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintSummaryHeader(const std::string &file_name) {
	std::stringstream log_message;
	GetSummary() << "\n" << GetSummaryCounter() << ". " << file_name << std::endl;
	log_message << PrintLineSep().str();
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise) {
	std::stringstream log_message;
	if (row_wise) {
		for (idx_t r = 0; r < values.size(); r++) {
			// fprintf(stderr, "%s\n", values[r].c_str());
			log_message << "\n" << values[r].c_str();
		}
	} else {
		idx_t c = 0;
		for (idx_t r = 0; r < values.size(); r++) {
			if (c != 0) {
				// fprintf(stderr, "\t");
				log_message << "\t";
			}
			// fprintf(stderr, "%s", values[r].c_str());
			log_message << values[r].c_str();
			c++;
			if (c >= columns) {
				// fprintf(stderr, "\n");
				log_message << std::endl;
				c = 0;
			}
		}
	}
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintLineSep() {
	string line_sep = string(80, '=');
	std::stringstream log_message;
	log_message << termcolor::color<128, 128, 128> << line_sep << termcolor::reset << std::endl;
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintHeader(string header) {
	std::stringstream log_message;
	log_message << termcolor::bold << header << termcolor::reset << std::endl;
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintFileHeader() {
	std::stringstream log_message;
	log_message << PrintHeader("File " + file_name + ":" + to_string(query_line) + ")").str();
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintSQL() {
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
	return Log(query);
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

std::stringstream SQLLogicTestLogger::PrintErrorHeader(const string &file_name, idx_t query_line,
                                                       const string &description) {
	std::stringstream log_message;
	log_message << PrintSummaryHeader(file_name).str();
	log_message << termcolor::red << termcolor::bold << description << " " << termcolor::reset;
	if (!file_name.empty()) {
		log_message << termcolor::bold << "(" << file_name << ":" << query_line << ")!" << termcolor::reset;
	}
	log_message << std::endl;
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintErrorHeader(const string &description) {
	return PrintErrorHeader(file_name, query_line, description);
}

std::stringstream SQLLogicTestLogger::PrintResultError(const vector<string> &result_values,
                                                       const vector<string> &values, idx_t expected_column_count,
                                                       bool row_wise) {
	std::stringstream log_message;
	log_message << PrintHeader("Expected result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintExpectedResult(values, expected_column_count, row_wise).str();
	log_message << PrintLineSep().str();
	log_message << PrintHeader("Actual result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintExpectedResult(result_values, expected_column_count, false).str();
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintResultString(MaterializedQueryResult &result) {
	std::stringstream log_message;
	log_message << result.ToString();
	// result.Print();
	return log_message;
}

std::stringstream SQLLogicTestLogger::PrintResultError(MaterializedQueryResult &result, const vector<string> &values,
                                                       idx_t expected_column_count, bool row_wise) {
	std::stringstream log_message;
	log_message << PrintHeader("Expected result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintExpectedResult(values, expected_column_count, row_wise).str();
	log_message << PrintLineSep().str();
	log_message << PrintHeader("Actual result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintResultString(result).str();
	return log_message;
}

void SQLLogicTestLogger::UnexpectedFailure(MaterializedQueryResult &result) {
	std::stringstream log_message;
	log_message << PrintLineSep().str();
	log_message << "Query unexpectedly failed (" << file_name << ":" << to_string(query_line) << ")\n";
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	log_message << PrintHeader("Actual result:").str();
	log_message << PrintResultString(result).str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}
void SQLLogicTestLogger::OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string) {
	std::stringstream log_message;
	// names
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			log_message << "\t";
		}
		log_message << result.names[c];
	}
	log_message << std::endl;
	// types
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			log_message << "\t";
		}
		log_message << result.types[c].ToString();
	}
	log_message << std::endl;
	PrintLineSep();
	for (idx_t r = 0; r < result.RowCount(); r++) {
		for (idx_t c = 0; c < result.ColumnCount(); c++) {
			if (c != 0) {
				log_message << "\t";
			}
			log_message << result_values_string[r * result.ColumnCount() + c];
		}
		log_message << std::endl;
	}
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::OutputHash(const string &hash_value) {
	std::stringstream log_message;
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	log_message << hash_value << std::endl;
	log_message << PrintLineSep().str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::ColumnCountMismatch(MaterializedQueryResult &result,
                                             const vector<string> &result_values_string, idx_t expected_column_count,
                                             bool row_wise) {

	std::stringstream log_message;
	log_message << PrintErrorHeader("Wrong column count in query!").str();
	log_message << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	            << termcolor::bold << result.ColumnCount() << termcolor::reset << " columns" << std::endl;
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	log_message << PrintResultError(result, result_values_string, expected_column_count, row_wise).str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count) {
	std::stringstream log_message;
	log_message << PrintErrorHeader("Error in test!").str();
	log_message << PrintLineSep().str();
	log_message
	    << "Expected " << to_string(expected_column_count) << " columns, but " << to_string(actual_column_count)
	    << " values were supplied\nThis is not cleanly divisible (i.e. the last row does not have enough values)";
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result,
                                       const vector<string> &comparison_values, idx_t expected_column_count,
                                       bool row_wise) {
	std::stringstream log_message;
	log_message << PrintErrorHeader("Wrong row count in query!").str();
	log_message << "Expected " << termcolor::bold << expected_rows << termcolor::reset << " rows, but got "
	            << termcolor::bold << result.RowCount() << termcolor::reset << " rows" << std::endl;
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	log_message << PrintResultError(result, comparison_values, expected_column_count, row_wise).str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
                                                          MaterializedQueryResult &result) {
	std::stringstream log_message;
	log_message << PrintLineSep().str();
	log_message << PrintErrorHeader("Wrong column count in query!").str();
	log_message << "Expected " << termcolor::bold << original_expected_columns << termcolor::reset
	            << " columns, but got " << termcolor::bold << expected_column_count << termcolor::reset << " columns"
	            << std::endl;
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	log_message << "The expected result " << termcolor::bold << "matched" << termcolor::reset << " the query result."
	            << std::endl;
	log_message << PrintLineSep().str();
	log_message << termcolor::bold << "Suggested fix: modify header to \"" << termcolor::green << "query "
	            << string(result.ColumnCount(), 'I') << termcolor::reset << termcolor::bold << "\"" << termcolor::reset
	            << std::endl;
	log_message << PrintLineSep().str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count) {
	std::stringstream log_message;
	log_message << PrintLineSep().str();
	log_message << PrintErrorHeader("Error in test! Column count mismatch after splitting on tab on row " +
	                                to_string(row_number) + "!")
	                   .str();
	log_message << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	            << termcolor::bold << split_count << termcolor::reset << " columns" << std::endl;
	log_message << "Does the result contain tab values? In that case, place every value on a single row." << std::endl;
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result) {
	std::stringstream log_message;
	if (expected_result) {
		expected_result->Print();
		expected_result->ToString();
	} else {
		log_message << "???\n";
	}
	log_message << PrintErrorHeader("Wrong result hash!").str();
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	log_message << PrintHeader("Expected result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintHeader("Actual result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintResultString(result).str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result) {
	std::stringstream log_message;
	log_message << PrintErrorHeader(!expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!").str();
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintLineSep().str();
	log_message << PrintResultString(result).str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result) {
	std::stringstream log_message;
	log_message << PrintErrorHeader("Query failed, but error message did not match expected error message: " +
	                                expected_error)
	                   .str();
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintHeader("Actual result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintResultString(result).str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::InternalException(MaterializedQueryResult &result) {
	std::stringstream log_message;
	log_message << PrintErrorHeader("Query failed with internal exception!").str();
	log_message << PrintLineSep().str();
	log_message << PrintSQL().str();
	log_message << PrintHeader("Actual result:").str();
	log_message << PrintLineSep().str();
	log_message << PrintResultString(result).str();
	std::cerr << log_message.str();
	GetSummary() << log_message.str();
}

void SQLLogicTestLogger::LoadDatabaseFail(const string &dbpath, const string &message) {
	PrintErrorHeader(string(), 0, "Failed to load database " + dbpath);
	PrintLineSep();
	Log("Error message: " + message + "\n");
	PrintLineSep();
}

} // namespace duckdb
