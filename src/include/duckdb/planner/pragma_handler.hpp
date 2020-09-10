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

namespace duckdb {
class ClientContext;
class SQLStatement;
struct PragmaInfo;

//! Pragma handler is responsible for converting certain pragma statements into new queries
class PragmaHandler {
public:
	PragmaHandler(ClientContext &context);

	void HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements);

private:
	ClientContext &context;

private:
	//! Handles a pragma statement, (potentially) returning a new statement to replace the current one
	string HandlePragma(PragmaInfo &pragma);

	void HandlePragmaStatementsInternal(vector<unique_ptr<SQLStatement>> &statements);
};
} // namespace duckdb
