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
#include "duckdb/common/fstream.hpp"

namespace duckdb {

class Command;
class LoopCommand;

class SQLLogicTestLogger {
public:
	SQLLogicTestLogger(ExecuteContext &context, const Command &command);
	~SQLLogicTestLogger();

	static void Log(const string &str);
	string PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise);
	static string PrintLineSep();
	static string PrintHeader(string header);
	string PrintFileHeader();
	string PrintSQL();
	void PrintSQLFormatted();
	string PrintErrorHeader(const string &description);
	static string PrintErrorHeader(const string &file_name, idx_t query_line, const string &description);
	string PrintResultError(const vector<string> &result_values, const vector<string> &values,
	                        idx_t expected_column_count, bool row_wise);
	string PrintResultError(MaterializedQueryResult &result, const vector<string> &values, idx_t expected_column_count,
	                        bool row_wise);
	string UnexpectedFailure(MaterializedQueryResult &result);
	void OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string);
	void OutputHash(const string &hash_value);
	string ColumnCountMismatch(MaterializedQueryResult &result, const vector<string> &result_values_string,
	                           idx_t expected_column_count, bool row_wise);
	string NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count);
	string WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result, const vector<string> &comparison_values,
	                     idx_t expected_column_count, bool row_wise);
	string ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
	                                        MaterializedQueryResult &result);
	string SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count);
	string WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result);
	string UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result);
	string ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result);
	string InternalException(MaterializedQueryResult &result);
	static void LoadDatabaseFail(const string &dbpath, const string &message);
	void AddToSummary(string log_message);

private:
	lock_guard<mutex> log_lock;
	string file_name;
	int query_line;
	string sql_query;
};
} // namespace duckdb
