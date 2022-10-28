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

	void Log(const string &str);
	void PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise);
	void PrintLineSep();
	void PrintHeader(string header);
	void PrintFileHeader();
	void PrintSQL();
	void PrintSQLFormatted();
	void PrintErrorHeader(const string &description);
	void PrintResultError(const vector<string> &result_values, const vector<string> &values,
	                      idx_t expected_column_count, bool row_wise);
	void PrintResultError(MaterializedQueryResult &result, const vector<string> &values, idx_t expected_column_count,
	                      bool row_wise);
	void UnexpectedFailure(MaterializedQueryResult &result);
	void OutputResult(MaterializedQueryResult &result, const vector<string> &result_values_string);
	void OutputHash(const string &hash_value);
	void ColumnCountMismatch(MaterializedQueryResult &result, const vector<string> &result_values_string,
	                         idx_t expected_column_count, bool row_wise);
	void NotCleanlyDivisible(idx_t expected_column_count, idx_t actual_column_count);
	void WrongRowCount(idx_t expected_rows, MaterializedQueryResult &result, const vector<string> &comparison_values,
	                   idx_t expected_column_count, bool row_wise);
	void ColumnCountMismatchCorrectResult(idx_t original_expected_columns, idx_t expected_column_count,
	                                      MaterializedQueryResult &result);
	void SplitMismatch(idx_t row_number, idx_t expected_column_count, idx_t split_count);
	void WrongResultHash(QueryResult *expected_result, MaterializedQueryResult &result);
	void UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result);
	void ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result);

private:
	lock_guard<mutex> log_lock;
	string file_name;
	int query_line;
	string sql_query;
};
} // namespace duckdb
