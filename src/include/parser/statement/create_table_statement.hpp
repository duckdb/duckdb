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

#include "parser/column_definition.hpp"

#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateTableStatement : public SQLStatement {
  public:
	CreateTableStatement()
	    : SQLStatement(StatementType::CREATE_TABLE),
	      info(make_unique<CreateTableInformation>()){};
	virtual ~CreateTableStatement() {
	}

	virtual std::string ToString() const {
		return "CREATE TABLE";
	}
	virtual std::unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	std::unique_ptr<CreateTableInformation> info;
};

} // namespace duckdb
