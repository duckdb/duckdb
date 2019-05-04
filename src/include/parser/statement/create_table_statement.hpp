//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateTableStatement : public SQLStatement {
public:
	CreateTableStatement() : SQLStatement(StatementType::CREATE_TABLE), info(make_unique<CreateTableInformation>()){};

	unique_ptr<CreateTableInformation> info;
	//! CREATE TABLE from QUERY
	unique_ptr<SelectStatement> query;
};

} // namespace duckdb
