//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/explain_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

class ExplainStatement : public SQLStatement {
public:
	ExplainStatement(unique_ptr<SQLStatement> stmt) : SQLStatement(StatementType::EXPLAIN), stmt(move(stmt)){};

	string ToString() const override {
		return "Explain";
	}
	
	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<SQLStatement> stmt;
};

} // namespace duckdb
