//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement/bound_simple_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

//! BoundSimpleStatement is a bound statement used for statements that do not need to be bound but only need to transfer
//! their ParseInfo further into the plan. This is typically true for schema statements (e.g. CREATE SEQUENCE, DROP
//! TABLE, etc...)
class BoundSimpleStatement : public BoundSQLStatement {
public:
	BoundSimpleStatement(StatementType type, unique_ptr<ParseInfo> info) : BoundSQLStatement(type), info(move(info)) {
	}

	unique_ptr<ParseInfo> info;

public:
	vector<string> GetNames() override {
		return {"Success"};
	}
	vector<SQLType> GetTypes() override {
		return {SQLType::BOOLEAN};
	}
};
} // namespace duckdb
