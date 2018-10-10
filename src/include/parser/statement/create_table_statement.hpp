//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/create_table_statement.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/column_definition.hpp"

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateTableStatement : public SQLStatement {
  public:
	CreateTableStatement()
	    : SQLStatement(StatementType::CREATE_TABLE),
	      info(make_unique<CreateTableInformation>()){};
	virtual ~CreateTableStatement() {}

	virtual std::string ToString() const { return "CREATE TABLE"; }
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	std::unique_ptr<CreateTableInformation> info;
};

} // namespace duckdb
