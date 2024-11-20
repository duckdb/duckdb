//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

//! Represents a TableReference to a base table in a catalog and schema.
class BaseTableRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::BASE_TABLE;

public:
	BaseTableRef()
	    : TableRef(TableReferenceType::BASE_TABLE), catalog_name(INVALID_CATALOG), schema_name(INVALID_SCHEMA) {
	}
	explicit BaseTableRef(const TableDescription &description)
	    : TableRef(TableReferenceType::BASE_TABLE), catalog_name(description.database), schema_name(description.schema),
	      table_name(description.table) {
	}

	//! The catalog name.
	string catalog_name;
	//! The schema name.
	string schema_name;
	//! The table name.
	string table_name;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;
	unique_ptr<TableRef> Copy() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
