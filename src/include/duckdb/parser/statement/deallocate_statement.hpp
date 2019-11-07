//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/deallocate_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class DeallocateStatement : public SQLStatement {
public:
	DeallocateStatement(string name) : SQLStatement(StatementType::DEALLOCATE), name(name){};

	string name;

	// TODO
};
} // namespace duckdb
