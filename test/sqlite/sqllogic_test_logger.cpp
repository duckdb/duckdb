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

string SQLLogicTestLogger::PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise) {
	string log_message = "";
	if (row_wise) {
		for (idx_t r = 0; r < values.size(); r++) {
			fprintf(stderr, "%s\n", values[r].c_str());
			log_message += "\n" + std::string(values[r].c_str());
		}
	} else {
		idx_t c = 0;
		for (idx_t r = 0; r < values.size(); r++) {
			if (c != 0) {
				fprintf(stderr, "\t");
				log_message += "\t";
			}
			fprintf(stderr, "%s", values[r].c_str());
			log_message += std::string(values[r].c_str());
			c++;
			if (c >= columns) {
				fprintf(stderr, "\n");
				log_message += "\n";
				c = 0;
			}
		}
	}
	return log_message;
}

string SQLLogicTestLogger::PrintLineSep() {
	string line_sep = string(80, '=');
	std::cerr << termcolor::color<128, 128, 128> << line_sep << termcolor::reset << std::endl;
	return line_sep + "\n";
}

string SQLLogicTestLogger::PrintHeader(string header) {
	std::cerr << termcolor::bold << header << termcolor::reset << std::endl;
	return header + "\n";
}

string SQLLogicTestLogger::PrintFileHeader() {
	string log_message = "File " + file_name + ":" + to_string(query_line) + ")";
	PrintHeader(log_message);
	return log_message + "\n";
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
	Log(query);
	return query;
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

string SQLLogicTestLogger::PrintErrorHeader(const string &file_name, idx_t query_line, const string &description) {
	string log_message = "next case\n" + file_name + "\n" + PrintLineSep() + description + " ";
	std::cerr << termcolor::red << termcolor::bold << description << " " << termcolor::reset;
	if (!file_name.empty()) {
		log_message += "(" + file_name + ":" + std::to_string(query_line) + ")!";
		std::cerr << termcolor::bold << "(" << file_name << ":" << query_line << ")!" << termcolor::reset;
	}
	std::cerr << std::endl;
	return log_message + "\n";
}

string SQLLogicTestLogger::PrintErrorHeader(const string &description) {
	return PrintErrorHeader(file_name, query_line, description);
}

string SQLLogicTestLogger::PrintResultError(const vector<string> &result_values, const vector<string> &values,
                                            idx_t expected_column_count, bool row_wise) {
	string log_message = PrintHeader("Expected result:");
	log_message += PrintLineSep();
	log_message += PrintExpectedResult(values, expected_column_count, row_wise);
	log_message += PrintLineSep();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	log_message += PrintExpectedResult(result_values, expected_column_count, false);
	return log_message;
}

string SQLLogicTestLogger::PrintResultError(MaterializedQueryResult &result, const vector<string> &values,
                                            idx_t expected_column_count, bool row_wise) {
	string log_message = PrintHeader("Expected result:");
	log_message += PrintLineSep();
	log_message += PrintExpectedResult(values, expected_column_count, row_wise);
	log_message += PrintLineSep();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	result.Print();
	log_message += result.ToString();
	return log_message;
}

string SQLLogicTestLogger::UnexpectedFailure(MaterializedQueryResult &result) {
	string log_message = PrintLineSep();
	log_message += "Query unexpectedly failed (" + file_name + ":" + to_string(query_line) + ")\n";
	std::cerr << "Query unexpectedly failed (" << file_name.c_str() << ":" << query_line << ")\n";
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintHeader("Actual result:");
	result.Print();
	log_message += result.ToString();
	return log_message;
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

string SQLLogicTestLogger::ColumnCountMismatch(MaterializedQueryResult &result,
                                             const vector<string> &result_values_string, idx_t expected_column_count,
                                             bool row_wise) {

	string log_message = PrintErrorHeader("Wrong column count in query!");
	std::cerr << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	          << termcolor::bold << result.ColumnCount() << termcolor::reset << " columns" << std::endl;
	log_message += "Expected " + to_string(expected_column_count) + " columns, but got " + to_string(result.ColumnCount()) + " columns\n";
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintResultError(result, result_values_string, expected_column_count, row_wise);
	return log_message;
}

string SQLLogicTestLogger::NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count) {
	string log_message = PrintErrorHeader("Error in test!");
	log_message += PrintLineSep();
	fprintf(stderr, "Expected %d columns, but %d values were supplied\n", (int)expected_column_count,
	        (int)actual_column_count);
	fprintf(stderr, "This is not cleanly divisible (i.e. the last row does not have enough values)\n");
	log_message += "Expected " + to_string(expected_column_count) + " columns, but " + to_string(actual_column_count) + " values were supplied\n";
	return log_message;
}

string SQLLogicTestLogger::WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result,
                                       const vector<string> &comparison_values, idx_t expected_column_count,
                                       bool row_wise) {
	string log_message = PrintErrorHeader("Wrong row count in query!");
	std::cerr << "Expected " << termcolor::bold << expected_rows << termcolor::reset << " rows, but got "
	          << termcolor::bold << result.RowCount() << termcolor::reset << " rows" << std::endl;
	log_message += "Expected " + to_string(expected_rows) + " rows, but got " + to_string(result.RowCount()) + " rows\n";
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += PrintResultError(result, comparison_values, expected_column_count, row_wise);
	return log_message;
}

string SQLLogicTestLogger::ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
                                                          MaterializedQueryResult &result) {
	string log_message = PrintLineSep();
	log_message += PrintErrorHeader("Wrong column count in query!");
	log_message += std::cerr << "Expected " << termcolor::bold << original_expected_columns << termcolor::reset << " columns, but got "
	          << termcolor::bold << expected_column_count << termcolor::reset << " columns" << std::endl;
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += std::cerr << "The expected result " << termcolor::bold << "matched" << termcolor::reset << " the query result."
	          << std::endl;
	log_message += std::cerr << termcolor::bold << "Suggested fix: modify header to \"" << termcolor::green << "query "
	          << string(result.ColumnCount(), 'I') << termcolor::reset << termcolor::bold << "\"" << termcolor::reset
	          << std::endl;
	log_message += "The expected result " + "matched" + " the query result.\n";
	log_message += "Suggested fix: modify header to \"" + "query " + string(result.ColumnCount(), 'I') + "\"\n";
	log_message += PrintLineSep();
	return log_message;
}

string SQLLogicTestLogger::SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count) {
	string log_message = PrintLineSep();
	log_message += PrintErrorHeader("Error in test! Column count mismatch after splitting on tab on row " + to_string(row_number) +
	                 "!");
	std::cerr << "Expected " << termcolor::bold << expected_column_count << termcolor::reset << " columns, but got "
	          << termcolor::bold << split_count << termcolor::reset << " columns" << std::endl;
	std::cerr << "Does the result contain tab values? In that case, place every value on a single row." << std::endl;
	log_message += "Expected " + to_string(expected_column_count) + " columns, but got " + to_string(split_count) + " columns\n";
	log_message += "Does the result contain tab values? In that case, place every value on a single row.\n";
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	return log_message;
}

string SQLLogicTestLogger::WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result) {
	string log_message = "";
	if (expected_result) {
		expected_result->Print();
		log_message += expected_result->ToString();
	} else {
		std::cerr << "???" << std::endl;
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
	result.Print();
	log_message += result.ToString();
	return log_message;
}

string SQLLogicTestLogger::UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result) {
	string log_message = PrintErrorHeader(!expect_ok ? "Query unexpectedly succeeded!" : "Query unexpectedly failed!");
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintLineSep();
	log_message += result.ToString() + "\n";
	result.Print();
	return log_message;
}

void SQLLogicTestLogger::ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result) {
	PrintErrorHeader("Query failed, but error message did not match expected error message: " + expected_error);
	PrintLineSep();
	PrintSQL();
	PrintHeader("Actual result:");
	PrintLineSep();
	result.Print();
}

string SQLLogicTestLogger::InternalException(MaterializedQueryResult &result) {
	string log_message = PrintErrorHeader("Query failed with internal exception!");
	log_message += PrintLineSep();
	log_message += PrintSQL();
	log_message += PrintHeader("Actual result:");
	log_message += PrintLineSep();
	result.Print();
	log_message += result.ToString();
	return log_message;
}

void SQLLogicTestLogger::LoadDatabaseFail(const string &dbpath, const string &message) {
	PrintErrorHeader(string(), 0, "Failed to load database " + dbpath);
	PrintLineSep();
	Log("Error message: " + message + "\n");
	PrintLineSep();
}

void SQLLogicTestLogger::AddToSummary(string log_message) {
	std::ofstream file("failures_summary.txt", std::ios::app);
	if (file.is_open()) {
		file << log_message;
		file.close();
	} else {
		std::cout << "Error opening failures_summary.txt file." << std::endl;
	}
}

} // namespace duckdb
