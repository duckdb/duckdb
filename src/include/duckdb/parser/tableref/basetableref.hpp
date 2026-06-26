//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/table_description.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

//! Represents a TableReference to a base table in a catalog and schema.
class BaseTableRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::BASE_TABLE;

public:
	BaseTableRef()
	    : TableRef(TableReferenceType::BASE_TABLE),
	      qualified_name(Identifier(INVALID_CATALOG), Identifier(INVALID_SCHEMA), Identifier()) {
	}
	explicit BaseTableRef(const TableDescription &description)
	    : TableRef(TableReferenceType::BASE_TABLE),
	      qualified_name(description.database, description.schema, description.table) {
	}

	//! The timestamp/version at which to read this table entry (if any)
	unique_ptr<AtClause> at_clause;

public:
	const QualifiedName &GetQualifiedName() const {
		return qualified_name;
	}
	QualifiedName &GetQualifiedNameMutable() {
		return qualified_name;
	}
	const Identifier &Catalog() const {
		return qualified_name.Catalog();
	}
	Identifier &CatalogMutable() {
		return qualified_name.CatalogMutable();
	}
	const Identifier &Schema() const {
		return qualified_name.Schema();
	}
	Identifier &SchemaMutable() {
		return qualified_name.SchemaMutable();
	}
	const Identifier &Table() const {
		return qualified_name.Name();
	}
	Identifier &TableMutable() {
		return qualified_name.NameMutable();
	}

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;
	unique_ptr<TableRef> Copy() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);

private:
	//! Qualified name of the base table (catalog.schema.table).
	QualifiedName qualified_name;
};

} // namespace duckdb
