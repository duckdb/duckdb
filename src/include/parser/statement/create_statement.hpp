
#pragma once

#include <vector>

#include "catalog/catalog.hpp"
#include "parser/statement/sql_statement.hpp"

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {

class CreateStatement : public SQLStatement {
  public:
	CreateStatement()
	    : SQLStatement(StatementType::CREATE), schema(DEFAULT_SCHEMA){};
	virtual ~CreateStatement() {}

	virtual std::string ToString() const;
	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	std::string table;
	std::string schema;

	std::vector<ColumnCatalogEntry> columns;
};

} // namespace duckdb
