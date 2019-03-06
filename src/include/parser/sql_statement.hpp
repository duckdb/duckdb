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
#include "common/printer.hpp"

namespace duckdb {
class SelectStatement;
class SQLNodeVisitor;

//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement {
public:
	SQLStatement(StatementType type) : type(type){};
	virtual ~SQLStatement() {
	}

	virtual bool Equals(const SQLStatement *other) const {
		return other && type == other->type;
	}

	virtual string ToString() const = 0;
	void Print() {
		Printer::Print(ToString());
	}

	StatementType type;
};
} // namespace duckdb
