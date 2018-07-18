
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
class ColumnRefExpression : public AbstractExpression {
  public:
	ColumnRefExpression() : AbstractExpression(ExpressionType::STAR) {}

	ColumnRefExpression(std::string column_name)
	    : AbstractExpression(ExpressionType::COLUMN_REF),
	      column_name(column_name) {}

	ColumnRefExpression(std::string column_name, std::string table_name)
	    : AbstractExpression(ExpressionType::COLUMN_REF),
	      column_name(column_name), table_name(table_name) {}

	const std::string &GetColumnName() const { return column_name; }
	const std::string &GetTableName() const { return table_name; }

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	std::string column_name;
	std::string table_name;
};
}
