
#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"

namespace duckdb {

class InsertStatement : public SQLStatement {
  public:
	InsertStatement() : SQLStatement(StatementType::INSERT){};
	virtual ~InsertStatement() {}
	virtual std::string ToString() const;
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	std::vector<std::unique_ptr<AbstractExpression>> values;

	std::string table;
	std::string schema;
};

} // namespace duckdb
