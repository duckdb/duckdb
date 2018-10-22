//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/drop_table_statement.hpp
//
// Author: Hannes Mühleisen
//
//===----------------------------------------------------------------------===//
#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropTableStatement : public SQLStatement {
  public:
	DropTableStatement()
	    : SQLStatement(StatementType::DROP_TABLE),
	      info(make_unique<DropTableInformation>()){};
	virtual ~DropTableStatement() {
	}

	virtual std::string ToString() const {
		return "DROP TABLE";
	}
	virtual void Accept(SQLNodeVisitor *v) {
		v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	std::unique_ptr<DropTableInformation> info;
};

} // namespace duckdb
