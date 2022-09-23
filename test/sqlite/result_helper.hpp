//===----------------------------------------------------------------------===//
//                         DuckDB
//
// result_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "sqllogic_command.hpp"

namespace duckdb {
class SQLLogicTestRunner;

class TestResultHelper {
public:
	TestResultHelper(Query &query, MaterializedQueryResult &result);
	TestResultHelper(Statement &stmt, MaterializedQueryResult &result);

	SQLLogicTestRunner &runner;
	MaterializedQueryResult &result;
	const string &file_name;
	idx_t query_line;
	const string &sql_query;

	// query result comparison
	Query *query_ptr;

	// statement result comparison
	bool expect_ok = true;

public:
	void CheckQueryResult(unique_ptr<MaterializedQueryResult> owned_result);
	void CheckStatementResult();

	static void PrintExpectedResult(vector<string> &values, idx_t columns, bool row_wise);
	string SQLLogicTestConvertValue(Value value, LogicalType sql_type, bool original_sqlite_test);
	void DuckDBConvertResult(MaterializedQueryResult &result, bool original_sqlite_test, vector<string> &out_result);
	static void PrintLineSep();
	static void PrintHeader(string header);
	static void PrintSQL(string sql);
	void PrintErrorHeader(const string &description);
	static void PrintResultError(vector<string> &result_values, vector<string> &values, idx_t expected_column_count,
	                             bool row_wise);
	static void PrintResultError(MaterializedQueryResult &result, vector<string> &values, idx_t expected_column_count,
	                             bool row_wise);

	static bool ResultIsHash(const string &result);
	static bool ResultIsFile(string result);

	bool CompareValues(string lvalue_str, string rvalue_str, idx_t current_row, idx_t current_column,
	                   vector<string> &values, idx_t expected_column_count, bool row_wise,
	                   vector<string> &result_values);
	bool SkipErrorMessage(const string &message);
	void ColumnCountMismatch(idx_t expected_column_count, bool row_wise);

	vector<string> LoadResultFromFile(string fname, vector<string> names);

private:
	TestResultHelper(Command &query, MaterializedQueryResult &result);
};

} // namespace duckdb
