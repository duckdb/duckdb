//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/sql_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/exception.hpp"
#include "common/printable.hpp"

namespace duckdb {
class SelectStatement;
class SQLNodeVisitor;

//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement : public Printable {
public:
	SQLStatement(StatementType type) : type(type){};
	virtual ~SQLStatement() {
	}

	virtual void Accept(SQLNodeVisitor *) = 0;
	virtual bool Equals(const SQLStatement *other) const {
		return other && type == other->type;
	}

	StatementType type;
};
} // namespace duckdb
