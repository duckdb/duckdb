//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/bound_sql_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {
//! Bound equivalent of SQLStatement
class BoundSQLStatement {
public:
	BoundSQLStatement(StatementType type) : type(type){};
	virtual ~BoundSQLStatement() {
	}

	StatementType type;
};
} // namespace duckdb
