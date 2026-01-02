//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/sql_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/named_parameter_map.hpp"

namespace duckdb {

//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::INVALID_STATEMENT;

public:
	explicit SQLStatement(StatementType type) : type(type) {
	}
	virtual ~SQLStatement() {
	}

	//! The statement type
	StatementType type;
	//! The statement location within the query string
	idx_t stmt_location = 0;
	//! The statement length within the query string
	idx_t stmt_length = 0;
	//! The map of named parameter to param index
	case_insensitive_map_t<idx_t> named_param_map;
	//! The query text that corresponds to this SQL statement
	string query;

protected:
	SQLStatement(const SQLStatement &other) = default;

public:
	virtual string ToString() const = 0;
	//! Create a copy of this SelectStatement
	DUCKDB_API virtual unique_ptr<SQLStatement> Copy() const = 0;

public:
public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE && TARGET::TYPE != StatementType::INVALID_STATEMENT) {
			throw InternalException("Failed to cast statement to type - statement type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE && TARGET::TYPE != StatementType::INVALID_STATEMENT) {
			throw InternalException("Failed to cast statement to type - statement type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};
} // namespace duckdb
