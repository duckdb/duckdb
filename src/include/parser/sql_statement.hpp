//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/sql_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/enums/statement_type.hpp"
#include "common/exception.hpp"
#include "common/printer.hpp"

namespace duckdb {
//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement {
public:
	SQLStatement(StatementType type) : type(type){};
	virtual ~SQLStatement() {
	}

	StatementType type;
};
} // namespace duckdb
