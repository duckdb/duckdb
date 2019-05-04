//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_schema_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateSchemaStatement : public SQLStatement {
public:
	CreateSchemaStatement()
	    : SQLStatement(StatementType::CREATE_SCHEMA), info(make_unique<CreateSchemaInformation>()){};

	unique_ptr<CreateSchemaInformation> info;
};

} // namespace duckdb
