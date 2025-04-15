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

string SQLLogicTestLogger::Log(const string &str) {
	string log_message;
	log_message += str;
	return log_message;
}

string SQLLogicTestLogger::PrintSummaryHeader(const std::string &file_name) {
	string log_message;
	GetSummary() << "\n" << GetSummaryCounter() << ". " << file_name << std::endl;
	log_message += PrintLineSep();
	return log_message;
}

string SQLLogicTestLogger::PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise) {
	string log_message;
	if (row_wise) {
		for (idx_t r = 0; r < values.size(); r++) {
			// fprintf(stderr, "%s\n", values[r].c_str());
			log_message += "\n" + values[r];
		}
	} else {
		idx_t c = 0;
		for (idx_t r = 0; r < values.size(); r++) {
			if (c != 0) {
				// fprintf(stderr, "\t");
				log_message += "\t";
			}
			// fprintf(stderr, "%s", values[r].c_str());
			log_message += values[r];
			c++;
			if (c >= columns) {
				// fprintf(stderr, "\n");
				log_message += "\n";
				c = 0;
			}
		}
	}
	return log_message;
}

string SQLLogicTestLogger::PrintLineSep() {
	string line_sep = string(80, '=');
	string log_message;
	std::ostringstream oss;
	oss << termcolor::color<128, 128, 128> << line_sep << termcolor::reset << std::endl;
	log_message = oss.str();
	return log_message;
}

string SQLLogicTestLogger::PrintHeader(string header) {
	string log_message;
	std::ostringstream oss;
	oss << termcolor::bold << header << termcolor::reset << std::endl;
	log_message += oss.str();
	return log_message;
}

string SQLLogicTestLogger::PrintFileHeader() {
	string log_message;
	log_message += PrintHeader("File " + file_name + ":" + to_string(query_line) + ")");
	return log_message;
}

string SQLLogicTestLogger::PrintSQL() {
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

string SQLLogicTestLogger::PrintErrorHeader(const string &file_name, idx_t query_line,
                                                       const string &description) {
	string log_message;
	std::ostringstream oss;
	log_message += PrintSummaryHeader(file_name);
	oss << termcolor::red << termcolor::bold << description << " " << termcolor::reset;
	if (!file_name.empty()) {
		oss << termcolor::bold << "(" << file_name << ":" << query_line << ")!" << termcolor::reset;
	}
	log_message += oss.str() + "\n";
	return log_message;
}

string SQLLogicTestLogger::PrintErrorHeader(const string &description) {
	return PrintErrorHeader(file_name, query_line, description);
}

string SQLLogicTestLogger::PrintResultError(const vector<string> &result_values,
                                                       const vector<string> &values, idx_t expected_column_count,
                                                       bool row_wise) {
	string log_message;
	log_message += PrintHeader("Expected result:");
	log_message += PrintLineSep();
	log_message += PrintExpectedResult(values, expected_column_count, row_wise);
	log_message += PrintLineSep();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	log_message += PrintExpectedResult(result_values, expected_column_count, false);
	return log_message;
}

string SQLLogicTestLogger::PrintResultString(MaterializedQueryResult &result) {
	string log_message;
	log_message += result.ToString();
	// result.Print();
	return log_message;
}

string SQLLogicTestLogger::PrintResultError(MaterializedQueryResult &result, const vector<string> &values,
                                                       idx_t expected_column_count, bool row_wise) {
	string log_message;
	log_message += PrintHeader("Expected result:");
	log_message += PrintLineSep();
	log_message += PrintExpectedResult(values, expected_column_count, row_wise);
	log_message += PrintLineSep();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	log_message += PrintResultString(result);
	return log_message;
}

void SQLLogicTestLogger::UnexpectedFailure(MaterializedQueryResult &result) {
	string log_message;
	std::ostringstream oss;
	log_message += PrintLineSep();
	oss << "Query unexpectedly failed (" << file_name << ":" << to_string(query_line) << ")\n";
	log_message += oss.str();
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintHeader("Actual result:");
	log_message += PrintResultString(result);
	std::cerr << log_message;
	GetSummary() << log_message;
}
void SQLLogicTestLogger::OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string) {
	string log_message;
	// names
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			log_message += "\t";
		}
		log_message += result.names[c];
	}
	log_message += "\n";
	// types
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		if (c != 0) {
			log_message += "\t";
		}
		log_message += result.types[c].ToString();
	}
	log_message += "\n";
	PrintLineSep();
	for (idx_t r = 0; r < result.RowCount(); r++) {
		for (idx_t c = 0; c < result.ColumnCount(); c++) {
			if (c != 0) {
				log_message += "\t";
			}
			log_message += result_values_string[r * result.ColumnCount() + c];
		}
		log_message += "\n";
	}
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::OutputHash(const string &hash_value) {
	string log_message;
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += hash_value + "\n";
	log_message += PrintLineSep();
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::ColumnCountMismatch(MaterializedQueryResult &result,
                                             const vector<string> &result_values_string, idx_t expected_column_count,
                                             bool row_wise) {

	string log_message;
	std::ostringstream oss;
	log_message += PrintErrorHeader("Wrong column count in query!");
	oss << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	            << termcolor::bold << result.ColumnCount() << termcolor::reset << " columns" << std::endl;
	log_message += oss.str();
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintResultError(result, result_values_string, expected_column_count, row_wise);
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count) {
	string log_message;
	std::ostringstream oss;
	log_message += PrintErrorHeader("Error in test!");
	log_message += PrintLineSep();
	oss
	    << "Expected " << to_string(expected_column_count) << " columns, but " << to_string(actual_column_count)
	    << " values were supplied\nThis is not cleanly divisible (i.e. the last row does not have enough values)";
	log_message += oss.str();
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result,
                                       const vector<string> &comparison_values, idx_t expected_column_count,
                                       bool row_wise) {
	string log_message;
	std::ostringstream oss;
	log_message += PrintErrorHeader("Wrong row count in query!");
	oss << "Expected " << termcolor::bold << expected_rows << termcolor::reset << " rows, but got "
	            << termcolor::bold << result.RowCount() << termcolor::reset << " rows" << std::endl;
	log_message += oss.str();
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintResultError(result, comparison_values, expected_column_count, row_wise);
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
                                                          MaterializedQueryResult &result) {
	string log_message;
	std::ostringstream oss;
	log_message += PrintLineSep();
	log_message += PrintErrorHeader("Wrong column count in query!");
	oss << "Expected " << termcolor::bold << original_expected_columns << termcolor::reset
	            << " columns, but got " << termcolor::bold << expected_column_count << termcolor::reset << " columns"
	            << std::endl;
	oss << PrintSQL();
	oss << PrintLineSep();
	oss << "The expected result " << termcolor::bold << "matched" << termcolor::reset << " the query result."
	            << std::endl;
	oss << PrintLineSep();
	oss << termcolor::bold << "Suggested fix: modify header to \"" << termcolor::green << "query "
	            << string(result.ColumnCount(), 'I') << termcolor::reset << termcolor::bold << "\"" << termcolor::reset
	            << std::endl;
	log_message += oss.str();
	log_message += PrintLineSep();
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count) {
	string log_message;
	std::ostringstream oss;
	log_message += PrintLineSep();
	log_message += PrintErrorHeader("Error in test! Column count mismatch after splitting on tab on row " +
	                                to_string(row_number) + "!")
	                   ;
	oss << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	            << termcolor::bold << split_count << termcolor::reset << " columns" << std::endl;
	log_message += oss.str();
	log_message += "Does the result contain tab values? In that case, place every value on a single row.\n";
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result) {
	string log_message;
	if (expected_result) {
		expected_result->Print();
		expected_result->ToString();
	} else {
		log_message += "???\n";
	}
	log_message += PrintErrorHeader("Wrong result hash!");
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintHeader("Expected result:");
	log_message += PrintLineSep();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	log_message += PrintResultString(result);
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result) {
	string log_message;
	log_message += PrintErrorHeader(!expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!");
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintResultString(result);
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result) {
	string log_message;
	log_message += PrintErrorHeader("Query failed, but error message did not match expected error message: " +
	                                expected_error)
	                   ;
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	log_message += PrintResultString(result);
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::InternalException(MaterializedQueryResult &result) {
	string log_message;
	log_message += PrintErrorHeader("Query failed with internal exception!");
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	log_message += PrintResultString(result);
	std::cerr << log_message;
	GetSummary() << log_message;
}

void SQLLogicTestLogger::LoadDatabaseFail(const string &dbpath, const string &message) {
	PrintErrorHeader(string(), 0, "Failed to load database " + dbpath);
	PrintLineSep();
	Log("Error message: " + message + "\n");
	PrintLineSep();
}

} // namespace duckdb
