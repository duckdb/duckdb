//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement_preprocessor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {
class ClientContext;
class ClientContextLock;
class SQLStatement;
struct PragmaInfo;

//! Preprocesses parsed statements: expands pragmas, unpacks multi-statements, and wraps in transactions
class StatementPreprocessor {
public:
	explicit StatementPreprocessor(ClientContext &context);

	void Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements,
	                bool is_in_active_transaction);

private:
	ClientContext &context;

private:
	//! Handles a pragma statement, returns whether the statement was expanded, if it was expanded the 'resulting_query'
	//! contains the statement(s) to replace the current one
	void TryExpandPragma(SQLStatement &statement, string &resulting_query, bool &expanded);
};
} // namespace duckdb
