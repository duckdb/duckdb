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
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include "parser/tableref/basetableref.hpp"

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
	virtual std::unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	//! The table to create the index on
	std::unique_ptr<BaseTableRef> table;
	//! Set of expressions to index by
	std::vector<std::unique_ptr<Expression>> expressions;
	// Info for index creation
	std::unique_ptr<CreateIndexInformation> info;
};

} // namespace duckdb
//
