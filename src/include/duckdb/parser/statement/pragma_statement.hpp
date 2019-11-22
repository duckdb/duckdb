//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/pragma_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

enum class PragmaType : uint8_t { NOTHING, ASSIGNMENT, CALL };

class PragmaStatement : public SQLStatement {
public:
	PragmaStatement() : SQLStatement(StatementType::PRAGMA) {};

	//! Name of the PRAGMA statement
	string name;
	//! Type of pragma statement
	PragmaType pragma_type;
	//! Parameter list (if any)
	vector<Value> parameters;
};

} // namespace duckdb
