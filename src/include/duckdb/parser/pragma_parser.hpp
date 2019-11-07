//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/pragma_parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class ClientContext;

enum class PragmaType : uint8_t { NOTHING, ASSIGNMENT, CALL };

//! Pragma parser is responsible for parsing PRAGMA statements separate from the Postgres parser
class PragmaParser {
public:
	PragmaParser(ClientContext &context);

	string new_query;

public:
	bool ParsePragma(string &query);

private:
	ClientContext &context;

private:
	void ParseMemoryLimit(string limit);
};
} // namespace duckdb
