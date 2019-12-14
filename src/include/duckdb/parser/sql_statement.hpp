//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/sql_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {
//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement {
public:
	SQLStatement(StatementType type) : type(type){};
	virtual ~SQLStatement() {
	}

	StatementType type;
	index_t stmt_location;
	index_t stmt_length;
};
} // namespace duckdb
