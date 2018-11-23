//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/alter_table_statement.hpp
//
// Author: Diego Tomé -  adapted from
// parser/statement/create_table_statement.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/expression.hpp"
#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

class AlterTableStatement : public SQLStatement {
  public:
	AlterTableStatement()
	    : SQLStatement(StatementType::ALTER),
	      info(make_unique<AlterTableInformation>()){};
	virtual ~AlterTableStatement() {
	}

	virtual std::string ToString() const {
		return "ALTER TABLE";
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

	std::unique_ptr<Expression> condition;
	std::unique_ptr<TableRef> table;
	std::unique_ptr<AlterTableInformation> info;

	std::vector<std::string> columns;
	std::vector<std::unique_ptr<Expression>> expressions;
};

} // namespace duckdb
