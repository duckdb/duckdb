//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/alter_table_statement.hpp
//
// Author: Diego Tomé
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
	AlterTableStatement(std::unique_ptr<AlterTableInformation> info)
	    : SQLStatement(StatementType::ALTER),
	      info(std::move(info)) { };
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

	std::unique_ptr<TableRef> table;
	std::unique_ptr<AlterTableInformation> info;
};

} // namespace duckdb
