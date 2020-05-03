//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/alter_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class AlterTableStatement : public SQLStatement {
public:
	AlterTableStatement() : SQLStatement(StatementType::ALTER_STATEMENT) {
	}
	AlterTableStatement(unique_ptr<AlterTableInfo> info)
	    : SQLStatement(StatementType::ALTER_STATEMENT), info(std::move(info)) {
	}

	unique_ptr<AlterTableInfo> info;
};

} // namespace duckdb
