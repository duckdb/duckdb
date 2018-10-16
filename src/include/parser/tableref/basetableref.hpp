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

	virtual std::unique_ptr<TableRef> Copy() override {
		auto copy = make_unique<BaseTableRef>();
		copy->database_name = database_name;
		copy->schema_name = schema_name;
		copy->table_name = table_name;
		copy->alias = alias;
		return copy;
	}

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
