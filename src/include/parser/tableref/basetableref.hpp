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

#include "parser/sql_node_visitor.hpp"
#include "parser/tableref/tableref.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BaseTableRef : public TableRef {
  public:
	BaseTableRef() : TableRef(TableReferenceType::BASE_TABLE) {}

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
