//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_schema_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class CreateSchemaStatement : public SQLStatement {
public:
	CreateSchemaStatement() : SQLStatement(StatementType::CREATE_SCHEMA), info(make_unique<CreateSchemaInfo>()){};

	unique_ptr<CreateSchemaInfo> info;
};

} // namespace duckdb
