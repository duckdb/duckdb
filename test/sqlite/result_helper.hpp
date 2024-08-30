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
class SQLLogicTestLogger;

class TestResultHelper {
public:
	TestResultHelper(SQLLogicTestRunner &runner) : runner(runner) {
	}

	SQLLogicTestRunner &runner;

public:
	bool CheckQueryResult(const Query &query, ExecuteContext &context,
	                      duckdb::unique_ptr<MaterializedQueryResult> owned_result);
	bool CheckStatementResult(const Statement &statement, ExecuteContext &context,
	                          duckdb::unique_ptr<MaterializedQueryResult> owned_result);
	string SQLLogicTestConvertValue(Value value, LogicalType sql_type, bool original_sqlite_test);
	void DuckDBConvertResult(MaterializedQueryResult &result, bool original_sqlite_test, vector<string> &out_result);

	static bool ResultIsHash(const string &result);
	static bool ResultIsFile(string result);

	bool MatchesRegex(SQLLogicTestLogger &logger, string lvalue_str, string rvalue_str);
	bool CompareValues(SQLLogicTestLogger &logger, MaterializedQueryResult &result, string lvalue_str,
	                   string rvalue_str, idx_t current_row, idx_t current_column, vector<string> &values,
	                   idx_t expected_column_count, bool row_wise, vector<string> &result_values);
	bool SkipErrorMessage(const string &message);

	vector<string> LoadResultFromFile(string fname, vector<string> names, idx_t &expected_column_count, string &error);
};

} // namespace duckdb
