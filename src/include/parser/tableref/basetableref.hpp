//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/tableref/basetableref.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BaseTableRef : public TableRef {
  public:
	BaseTableRef() : TableRef(TableReferenceType::BASE_TABLE) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual bool Equals(const TableRef *other_) override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (BaseTableRef *)other_;
		return other->schema_name == schema_name &&
		       other->table_name == table_name;
	}

	virtual std::unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a BaseTableRef
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a BaseTableRef
	static std::unique_ptr<TableRef> Deserialize(Deserializer &source);

	virtual std::string ToString() const override {
		return "GET(" + schema_name + "." + table_name + ")";
	}

	//! Schema name
	std::string schema_name;
	//! Table name
	std::string table_name;
};
} // namespace duckdb
