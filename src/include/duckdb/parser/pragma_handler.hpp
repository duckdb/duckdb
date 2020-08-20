//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/pragma_handler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
struct PragmaInfo;

//! Pragma handler is responsible for converting certain pragma statements into new queries
class PragmaHandler {
public:
	//! Handles a pragma statement, (potentially) returning a new statement to replace the current one
	string HandlePragma(PragmaInfo &pragma);

private:
	void ParseMemoryLimit(string limit);
};
} // namespace duckdb
