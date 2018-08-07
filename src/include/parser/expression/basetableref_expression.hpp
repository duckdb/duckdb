//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/basetableref_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BaseTableRefExpression : public TableRefExpression {
  public:
	BaseTableRefExpression()
	    : TableRefExpression(TableReferenceType::BASE_TABLE) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	//! Database name, not used
	std::string database_name;
	//! Schema name
	std::string schema_name;
	//! Table name
	std::string table_name;

	virtual std::string ToString() const override {
		return "GET(" + schema_name + "." + table_name + ")";
	}
};
} // namespace duckdb
