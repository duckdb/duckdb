//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqllogic_test_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "sqllogic_command.hpp"

namespace duckdb {

class Command;
class LoopCommand;

class SQLLogicTestLogger {
public:
	SQLLogicTestLogger(ExecuteContext &context, const Command &command);
	~SQLLogicTestLogger();

	static void Log(const string &str, std::ostringstream &oss);
	static void PrintSummaryHeader(const std::string &file_name, std::ostringstream &oss);
	void PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise, std::ostringstream &oss);
	static void PrintLineSep(std::ostringstream &oss);
	static void PrintHeader(string header, std::ostringstream &oss);
	void PrintFileHeader(std::ostringstream &oss);
	void PrintSQL(std::ostringstream &oss);
	void PrintSQLFormatted();
	void PrintErrorHeader(const string &description, std::ostringstream &oss);
	static void PrintErrorHeader(const string &file_name, idx_t query_line, const string &description,
	                             std::ostringstream &oss);
	void PrintResultError(const vector<string> &result_values, const vector<string> &values,
	                      idx_t expected_column_count, bool row_wise, std::ostringstream &oss);
	void PrintResultError(MaterializedQueryResult &result, const vector<string> &values, idx_t expected_column_count,
	                      bool row_wise, std::ostringstream &oss);
	void PrintResultString(MaterializedQueryResult &result, std::ostringstream &oss);
	void UnexpectedFailure(MaterializedQueryResult &result, std::ostringstream &oss);
	void OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string,
	                  std::ostringstream &oss);
	void OutputHash(const string &hash_value, std::ostringstream &oss);
	void ColumnCountMismatch(MaterializedQueryResult &result, const vector<string> &result_values_string,
	                         idx_t expected_column_count, bool row_wise, std::ostringstream &oss);
	void NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count, std::ostringstream &oss);
	void WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result, const vector<string> &comparison_values,
	                   idx_t expected_column_count, bool row_wise, std::ostringstream &oss);
	void ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
	                                      MaterializedQueryResult &result, std::ostringstream &oss);
	void SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count, std::ostringstream &oss);
	void WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result, std::ostringstream &oss);
	void UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result, std::ostringstream &oss);
	void ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result, std::ostringstream &oss);
	void InternalException(MaterializedQueryResult &result, std::ostringstream &oss);
	static void LoadDatabaseFail(const string &dbpath, const string &message, std::ostringstream &oss);

private:
	lock_guard<mutex> log_lock;
	string file_name;
	int query_line;
	string sql_query;
};
} // namespace duckdb
