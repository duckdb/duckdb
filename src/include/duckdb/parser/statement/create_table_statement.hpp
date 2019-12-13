//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

class CreateTableStatement : public SQLStatement {
public:
	CreateTableStatement() : SQLStatement(StatementType::CREATE_TABLE), info(make_unique<CreateTableInfo>()){};

	unique_ptr<CreateTableInfo> info;
	//! CREATE TABLE from QUERY
	unique_ptr<SelectStatement> query;
};

} // namespace duckdb
