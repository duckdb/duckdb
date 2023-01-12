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
#include "duckdb/common/named_parameter_map.hpp"

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
	idx_t stmt_location = 0;
	//! The statement length within the query string
	idx_t stmt_length = 0;
	//! The number of prepared statement parameters (if any)
	idx_t n_param = 0;
	//! The map of named parameter to param index (if n_param and any named)
	case_insensitive_map_t<idx_t> named_param_map;
	//! The query text that corresponds to this SQL statement
	string query;

protected:
	SQLStatement(const SQLStatement &other) = default;

public:
	virtual string ToString() const {
		throw InternalException("ToString not supported for this type of SQLStatement: '%s'",
		                        StatementTypeToString(type));
	}
	//! Create a copy of this SelectStatement
	virtual unique_ptr<SQLStatement> Copy() const = 0;
};
} // namespace duckdb
