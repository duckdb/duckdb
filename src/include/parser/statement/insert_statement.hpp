//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/insert_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/statement/select_statement.hpp"

#include <vector>

namespace duckdb {

class InsertStatement : public SQLStatement {
public:
	InsertStatement() : SQLStatement(StatementType::INSERT), schema(DEFAULT_SCHEMA){};
	virtual ~InsertStatement() {
	}

	virtual string ToString() const;
	virtual unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) const {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	//! The select statement to insert from
	unique_ptr<SelectStatement> select_statement;

	//! List of values to insert
	vector<vector<unique_ptr<Expression>>> values;

	//! Column names to insert into
	vector<string> columns;

	//! Table name to insert to
	string table;
	//! Schema name to insert to
	string schema;
};

} // namespace duckdb
