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
	explicit SQLStatement(StatementType type) : type(type) {};
	virtual ~SQLStatement() {
	}

	//! The statement type
	StatementType type;
	//! The statement location within the query string
	idx_t stmt_location;
	//! The statement length within the query string
	idx_t stmt_length;
	//! The number of prepared statement parameters (if any)
	idx_t n_param;
	//! The query text that corresponds to this SQL statement
	string query;

protected:
	SQLStatement(const SQLStatement &other) = default;

public:
	virtual string ToString() const {
		throw InternalException("ToString not supported for this type of SQLStatement");
	}
	//! Create a copy of this SelectStatement
	virtual unique_ptr<SQLStatement> Copy() const = 0;
};
} // namespace duckdb
