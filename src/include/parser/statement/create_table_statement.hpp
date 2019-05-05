//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/parsed_data/create_table_info.hpp"
#include "parser/sql_statement.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {

class CreateTableStatement : public SQLStatement {
public:
	CreateTableStatement() : SQLStatement(StatementType::CREATE_TABLE), info(make_unique<CreateTableInfo>()){};

	unique_ptr<CreateTableInfo> info;
	//! CREATE TABLE from QUERY
	unique_ptr<SelectStatement> query;
};

} // namespace duckdb
