//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/statement/explain_statement.hpp
//
// Author: Hannes Mühleisen
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/sql_statement.hpp"

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class ExplainStatement : public SQLStatement {
  public:
	ExplainStatement(std::unique_ptr<SQLStatement> stmt)
	    : SQLStatement(StatementType::EXPLAIN), stmt(move(stmt)){};
	std::unique_ptr<SQLStatement> stmt;

	virtual ~ExplainStatement() {
	}

	virtual std::string ToString() const {
		return "Explain";
	}
	virtual std::unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return nullptr;
	}

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}
};

} // namespace duckdb
