
#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
class BaseTableRefExpression : public TableRefExpression {
  public:
	BaseTableRefExpression()
	    : TableRefExpression(TableReferenceType::BASE_TABLE) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	std::string database_name;
	std::string schema_name;
	std::string table_name;
};
} // namespace duckdb
