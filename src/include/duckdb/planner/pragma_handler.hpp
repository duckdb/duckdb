//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/pragma_handler.hpp
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

//! Pragma handler is responsible for converting certain pragma statements into new queries
class PragmaHandler {
public:
	explicit PragmaHandler(ClientContext &context);

	void HandlePragmaStatements(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements);

private:
	ClientContext &context;

private:
	//! Handles a pragma statement, returns whether the statement was expanded, if it was expanded the 'resulting_query'
	//! contains the statement(s) to replace the current one
	bool HandlePragma(SQLStatement *statement, string &resulting_query);

	void HandlePragmaStatementsInternal(vector<unique_ptr<SQLStatement>> &statements);
};
} // namespace duckdb
