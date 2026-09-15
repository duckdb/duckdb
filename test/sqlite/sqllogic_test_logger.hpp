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
#include "test_failure_record.hpp"

namespace duckdb {

class Command;
class LoopCommand;

class SQLLogicTestLogger {
public:
	SQLLogicTestLogger(ExecuteContext &context, const Command &command);
	~SQLLogicTestLogger();

	static void Log(const string &annotation, const string &str);
	static void PrintSkip(const string &file_name, const string &reason);
	//! Emit the parseable skip marker and record the skip with the reporter
	static void ReportSkip(const string &file_name, const string &reason);
	void PrintExpectedResult(const vector<string> &values, idx_t columns, bool row_wise);
	static void PrintLineSep();
	static void PrintHeader(string header);
	void PrintFileHeader();
	void PrintSQL();
	void PrintSQLFormatted();
	void PrintErrorHeader(const string &description);
	static void PrintErrorHeader(const string &file_name, idx_t query_line, const string &description);
	void PrintResultError(const vector<string> &result_values, const vector<string> &values,
	                      idx_t expected_column_count, bool row_wise);
	static void PrintSummaryHeader(const std::string &file_name, idx_t query_line);
	void PrintResultError(MaterializedQueryResult &result, const vector<string> &values, idx_t expected_column_count,
	                      bool row_wise);
	void PrintResultString(MaterializedQueryResult &result);
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
	void WrongResultHash(const string &expected_result, MaterializedQueryResult &result, const string &expected_hash,
	                     const string &actual_hash);
	void UnexpectedStatement(bool expect_ok, MaterializedQueryResult &result);
	void ExpectedErrorMismatch(const string &expected_error, MaterializedQueryResult &result);
	void InternalException(MaterializedQueryResult &result);
	//! A single cell differed. `row`/`column` are zero-based indices into the compared result.
	void ValueMismatch(MaterializedQueryResult &result, const string &actual_value, const string &expected_value,
	                   idx_t row, idx_t column, const vector<string> &result_values, const vector<string> &values,
	                   idx_t expected_column_count, bool row_wise);
	void TestError(const string &description, const string &detail);
	static void LoadDatabaseFail(const string &file_name, const string &dbpath, const string &message);
	//! Write a machine-readable event line: "[TEST_EVENT] <json>" (--emit-test-events). Caller gates.
	static void EmitTestEvent(const string &json_payload);

	static void AppendFailure(const string &log_message);
	static void LogFailure(const string &log_message);
	static void LogFailureAnnotation(const string &log_message);
	string ResultToString(MaterializedQueryResult &result);

private:
	//! Seed a record with this failure's location and query. Cheap enough to build unconditionally;
	//! callers still gate on TestFailureRecorder::Enabled() before doing the result-copying work.
	TestFailureRecord BaseRecord(TestFailureKind kind, const string &message) const;
	//! Copy a row-major value list into `out`, capping it at TEST_FAILURE_MAX_VALUES cells.
	static void FillValues(const vector<string> &values, idx_t columns, vector<string> &out, bool &truncated,
	                       idx_t &rows);
	//! Copy a query result into `out` in the same row-major layout, under the same cap.
	void FillValues(MaterializedQueryResult &result, vector<string> &out, bool &truncated, idx_t &rows, idx_t &columns);
	//! Row indices where `expected` and `actual` differ. Empty unless both sides share a shape.
	static vector<idx_t> ComputeMismatchRows(const vector<string> &expected, const vector<string> &actual,
	                                         idx_t columns);

private:
	Connection &connection;
	lock_guard<mutex> log_lock;
	string file_name;
	int query_line;
	string sql_query;
};
} // namespace duckdb
