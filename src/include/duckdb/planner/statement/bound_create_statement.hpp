//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_create_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/parsed_data/bound_create_info.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"

namespace duckdb {
//! Bound equivalent to CreateStatement
class BoundCreateStatement : public BoundSQLStatement {
public:
	BoundCreateStatement() : BoundSQLStatement(StatementType::CREATE) {
	}

	// Info for element creation
	unique_ptr<BoundCreateInfo> info;

public:
	vector<string> GetNames() override {
		return {"Count"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::BIGINT};
	}
};
} // namespace duckdb
