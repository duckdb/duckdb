//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/pragma_handler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class ClientContext;
class SQLStatement;
struct PragmaInfo;

//! Pragma handler is responsible for handling PRAGMA statements
class PragmaHandler {
public:
	PragmaHandler(ClientContext &context);

public:
	//! Handles a pragma statement, (potentially) returning a new statement to replace the current one
	unique_ptr<SQLStatement> HandlePragma(PragmaInfo &pragma);

private:
	ClientContext &context;

private:
	void ParseMemoryLimit(string limit);
};
} // namespace duckdb
