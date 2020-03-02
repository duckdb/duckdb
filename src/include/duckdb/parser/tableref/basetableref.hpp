//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BaseTableRef : public TableRef {
public:
	BaseTableRef() : TableRef(TableReferenceType::BASE_TABLE), schema_name(INVALID_SCHEMA) {
	}

	//! Schema name
	string schema_name;
	//! Table name
	string table_name;

public:
	string ToString() const override {
		return "GET(" + schema_name + "." + table_name + ")";
	}

	bool Equals(const TableRef *other_) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a BaseTableRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a BaseTableRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
