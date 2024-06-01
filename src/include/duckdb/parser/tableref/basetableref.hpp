//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BaseTableRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::BASE_TABLE;

public:
	BaseTableRef()
	    : TableRef(TableReferenceType::BASE_TABLE), catalog_name(INVALID_CATALOG), schema_name(INVALID_SCHEMA) {
	}

	//! The catalog name
	string catalog_name;
	//! Schema name
	string schema_name;
	//! Table name
	string table_name;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a BaseTableRef
	void Serialize(Serializer &serializer) const override;

	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
