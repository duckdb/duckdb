//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/create_index_statement.hpp
//
// Author: Pedro Holanda
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateIndexStatement : public SQLStatement {
  public:
	CreateIndexStatement()
	    : SQLStatement(StatementType::CREATE_INDEX),
	      info(make_unique<CreateIndexInformation>()){};
	virtual ~CreateIndexStatement() {
	}

	virtual std::string ToString() const {
		return "CREATE INDEX";
	}
	virtual void Accept(SQLNodeVisitor *v) {
		//            v->Visit(*this);
	}

	std::unique_ptr<CreateIndexInformation> info;
};

} // namespace duckdb
//
