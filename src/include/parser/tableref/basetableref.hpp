//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref/basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BaseTableRef : public TableRef {
public:
	BaseTableRef() : TableRef(TableReferenceType::BASE_TABLE), schema_name(DEFAULT_SCHEMA) {
	}

	unique_ptr<TableRef> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	bool Equals(const TableRef *other_) const override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (BaseTableRef *)other_;
		return other->schema_name == schema_name && other->table_name == table_name;
	}

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a BaseTableRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a BaseTableRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);

	string ToString() const override {
		return "GET(" + schema_name + "." + table_name + ")";
	}

	//! Schema name
	string schema_name;
	//! Table name
	string table_name;
};
} // namespace duckdb
